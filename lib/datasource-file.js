/*
 * lib/datasource-file.js: implementation of Datasource for file-based data
 * sources
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_vasync = require('vasync');
var mod_vstream = require('vstream');
var CatStreams = require('catstreams');
var VError = require('verror');

var mod_dragnet_impl = require('./dragnet-impl');
var mod_pathenum = require('./path-enum');
var mod_streamutil = require('./stream-util');
var FindStream = require('./fs-find');
var IndexSink = require('./index-sink');
var StreamScan = require('./stream-scan');

/* Public interface */
exports.createDatasource = createDatasource;

function createDatasource(args)
{
	var dsconfig;

	mod_assertplus.object(args);
	mod_assertplus.object(args.dsconfig);
	mod_assertplus.object(args.log);

	dsconfig = args.dsconfig;
	mod_assertplus.equal(dsconfig.ds_backend, 'file');
	if (typeof (dsconfig.ds_backend_config.path) != 'string')
		return (new VError('expected datasource "path" ' +
		    'to be a string'));
	return (new DatasourceFile(args));
}

function DatasourceFile(args)
{
	this.ds_format = args.dsconfig.ds_format;
	this.ds_timeformat = args.dsconfig.ds_backend_config.timeFormat || null;
	this.ds_datapath = args.dsconfig.ds_backend_config.path;
	this.ds_indexpath = args.dsconfig.ds_backend_config.indexPath || null;
	this.ds_log = args.log;
}

/*
 * [public] Clean up any resources opened by this datasource.
 */
DatasourceFile.prototype.close = function ()
{
};

/*
 * [public] Scan raw data to execute a query.  Arguments:
 *
 *     query		describes the query the user wants to execute
 *
 *     dryRun		if true, just print what would be done
 */
DatasourceFile.prototype.scan = function (args)
{
	var scanctx, datastream, scan;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');

	scanctx = this.scanInit({
	    'dryRun': args.dryRun,
	    'timeBefore': args.query.qc_before,
	    'timeAfter': args.query.qc_after
	});

	if (scanctx instanceof Error)
		return (this.asyncError(scanctx));

	if (args.dryRun)
		return (scanctx.outstream);

	/*
	 * Wire up the pipeline.  Serious errors and warnings are propagated by
	 * the vstream framework to the caller.  Hang a few fields off the tail
	 * stream for debugging.
	 */
	datastream = this.dataStream(scanctx.findstream);
	scan = new StreamScan({
	    'query': args.query,
	    'log': this.ds_log.child({ 'component': 'scan' })
	});
	datastream.pipe(scanctx.parser);
	scanctx.parser.pipe(scan);
	scan.ds_datapath = this.ds_datapath;
	return (scan);
};

/*
 * Generate prerequisites for both scanning and building an index.  May return
 * an error synchronously.  If dryRun is set, then this also sets up execution
 * of the dry run.  (The caller can return without doing anything else.)
 * Arguments:
 *
 *     dryRun		same as everywhere else
 *
 *     [timeBefore]	same as everywhere else
 *
 *     [timeAfter]	same as everywhere else
 */
DatasourceFile.prototype.scanInit = function (args)
{
	var error, path, findstream, parser, rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	error = null;
	if (this.ds_timeformat === null &&
	    (args.timeBefore !== null || args.timeAfter !== null)) {
		error = new VError('datasource is missing "timeformat" ' +
		    'for "before" and "after" constraints');
	}

	if (error !== null)
		return (error);

	/*
	 * The pipeline consists of enumerating directories, finding files in
	 * those directories, concatenating their contents, parsing the results,
	 * and then doing something with the data stream that varies depending
	 * on the caller.
	 */
	parser = mod_dragnet_impl.parserFor(this.ds_format);
	if (parser instanceof Error)
		return (parser);

	mod_assertplus.string(this.ds_datapath);
	path = this.ds_datapath;
	findstream = this.findStream(path, this.ds_timeformat,
	    args.timeAfter || null, args.timeBefore || null);
	if (findstream instanceof Error)
		return (findstream);

	if (args.dryRun) {
		rv = new mod_stream.PassThrough();
		console.error('would scan files:');
		findstream.on('data', function (fileinfo) {
			if (fileinfo.error)
				return;
			console.error('    %s', fileinfo.path);
		});
		findstream.on('end', function () { rv.end(); });
		return ({ 'outstream': rv });
	}

	return ({
	    'parser': parser,
	    'findstream': findstream
	});
};

/*
 * Given an error, return a stream that will emit that error.  This is used in
 * situations where we want to emit an error from an interface that emits errors
 * asynchronously.
 */
DatasourceFile.prototype.asyncError = function (err)
{
	var rv = new mod_stream.PassThrough();
	setImmediate(rv.emit.bind(rv, 'error', err));
	return (rv);
};

/*
 * Generate a stream that will emit the names of files that should be scanned
 * for the given query.  These files may be either raw data files or indexes.
 */
DatasourceFile.prototype.findStream = function (root, timeformat, start, end)
{
	var finder, pathenum;

	mod_assertplus.string(root, 'root');
	finder = new FindStream({
	    'log': this.ds_log.child({ 'component': 'find' })
	});

	if (end === null) {
		finder.write(root);
		finder.end();
		return (finder);
	}

	mod_assertplus.ok(start !== null);
	mod_assertplus.string(timeformat, 'timeformat');
	pathenum = mod_pathenum.createPathEnumerator({
	    'pattern': mod_path.join(root, timeformat),
	    'timeStart': start,
	    'timeEnd': end
	});

	if (pathenum instanceof Error)
		return (pathenum);

	pathenum.pipe(finder);
	return (finder);
};

/*
 * Given a FindStream emitting raw data filenames, attach a pipeline that will
 * read the raw data from disk (as bytes).
 */
DatasourceFile.prototype.dataStream = function (findstream)
{
	var catstream;

	/*
	 * XXX catstreams should really be writable so that we can pipe this and
	 * get flow control.  For now, we may end up buffering the names of all
	 * files found.  Because of this, we also have to use vsRecordPipe
	 * directly.
	 */
	catstream = new CatStreams({
	    'log': this.ds_log,
	    'perRequestBuffer': 16834,
	    'maxConcurrency': 2
	});
	catstream = mod_vstream.wrapStream(catstream);

	findstream.vsRecordPipe(catstream);
	findstream.on('data', function (fileinfo) {
		if (fileinfo.error) {
			catstream.emit('find_error', fileinfo);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile());
		catstream.cat(function () {
			return (mod_fs.createReadStream(fileinfo.path));
		});
	});

	findstream.on('end', function () {
		catstream.cat(null);
	});

	return (catstream);
};

/*
 * [public] Build an index to support the given metrics.  Arguments:
 *
 *     metrics		Non-empty array of metrics that should be supported, as
 *     			metric configuration objects.
 *
 *     dryRun		If true, just print out what would be done.
 *
 *     interval		How to chunk up indexes (e.g., 'hour', 'day').
 *
 *     [timeAfter]	If specified, only scan data after this timestamp.
 *
 *     [timeBefore]	If specified, only scan data before this timestamp.
 *
 * See the Manta-based implementation for information about the index
 * configuration file.
 */
DatasourceFile.prototype.build = function (args, callback)
{
	var error, scanctx, metrics, datastream, queries, barrier, sink, i;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.arrayOfObject(args.metrics, 'args.metrics');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.string(args.interval, 'args.interval');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	error = null;
	if (args.timeAfter !== null && args.timeBefore === null)
		error = new VError('cannot specify --after without --before');
	else if (args.timeBefore !== null && args.timeAfter === null)
		error = new VError('cannot specify --before without --after');
	else if (this.ds_indexpath === null)
		error = new VError('datasource is missing "indexpath"');

	switch (args.interval) {
	case 'all':
		break;

	default:
		error = new VError(
		    'interval "%s" not supported', args.interval);
		break;
	}

	if (error === null) {
		scanctx = this.scanInit({
		    'dryRun': args.dryRun,
		    'timeAfter': args.timeAfter,
		    'timeBefore': args.timeBefore
		});
		if (scanctx instanceof Error)
			error = scanctx;
	}

	if (error !== null) {
		setImmediate(callback, error);
		return (null);
	}

	if (args.dryRun) {
		scanctx.outstream.on('end', callback);
		return (null);
	}

	/*
	 * Construct the sink, which will read skinner-style data points and
	 * write them into a sqlite index.
	 */
	metrics = args.metrics;
	sink = this.indexSink({
	    'metrics': metrics,
	    'interval': args.interval
	});

	/*
	 * Now that we've got a data stream, create separate Query configs for
	 * each metric that should be supported by this index.  Pipe the data
	 * stream to a StreamScanner for each query, and pipe all of those back
	 * into a single IndexSink.
	 */
	datastream = this.dataStream(scanctx.findstream);
	datastream.pipe(scanctx.parser);
	queries = metrics.map(function (m) {
		return (mod_dragnet_impl.metricQuery(m,
		    args.timeAfter, args.timeBefore));
	});

	for (i = 0; i < queries.length; i++) {
		if (queries[i] instanceof Error) {
			setImmediate(callback, queries[i]);
			return (null);
		}
	}

	barrier = mod_vasync.barrier();
	barrier.start('loop');
	queries.forEach(function (q, qi) {
		var s, t;

		s = new StreamScan({
		    'query': q,
		    'log': self.ds_log.child({ 'metric': qi })
		});

		t = mod_vstream.wrapStream(mod_streamutil.transformStream({
		    'streamOptions': { 'objectMode': true, 'highWaterMark': 0 },
		    'func': function (chunk, _, xformcb) {
			chunk['fields']['__dn_metric'] = qi;
			this.push(chunk);
			setImmediate(xformcb);
		    }
		}));

		scanctx.parser.pipe(s);
		s.pipe(t);
		t.pipe(sink, { 'end': false });

		barrier.start('query ' + qi);
		t.on('end', function () { barrier.done('query ' + qi); });
	});

	sink.on('finish', callback);
	barrier.on('drain', function () { sink.end(); });
	barrier.done('loop');
	return (sink);
};

/*
 * An index sink is a Writable stream that reads skinner-style data points and
 * writes them into a sqlite index.  For now, we only support the "all" sink,
 * which doesn't try to chunk up the indexes based on timestamp.  Arguments:
 *
 *     metrics		See build().
 *
 *     interval		See build().
 */
DatasourceFile.prototype.indexSink = function (args)
{
	var sink;

	mod_assertplus.equal(args.interval, 'all');
	sink = new IndexSink({
	    'log': this.ds_log,
	    'metrics': args.metrics,
	    'filename': this.ds_indexpath
	});
	return (sink);
};
