/*
 * lib/datasource-file.js: implementation of Datasource for file-based data
 * sources
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_krill = require('krill');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_vasync = require('vasync');
var mod_vstream = require('vstream');
var CatStreams = require('catstreams');
var PipelineStream = mod_vstream.PipelineStream;
var VError = require('verror');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_dragnet_impl = require('./dragnet-impl');
var mod_pathenum = require('./path-enum');
var mod_streamutil = require('./stream-util');
var FindStream = require('./fs-find');
var IndexSink = require('./index-sink');
var IndexQuerier = require('./index-query');
var KrillSkinnerStream = require('./krill-skinner-stream');
var MultiplexStream = require('./stream-multiplex');
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
	this.ds_timefield = args.dsconfig.ds_backend_config.timeField || null;
	this.ds_datapath = args.dsconfig.ds_backend_config.path;
	this.ds_indexpath = args.dsconfig.ds_backend_config.indexPath || null;
	this.ds_filter = args.dsconfig.ds_filter || null;
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
	    'filter': this.ds_filter,
	    'dryRun': args.dryRun,
	    'timeBefore': args.query.qc_before,
	    'timeAfter': args.query.qc_after
	});

	if (scanctx instanceof Error)
		return (mod_dragnet_impl.asyncError(scanctx));

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
	    'timeField': this.ds_timefield,
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
 *     [filter]		same as everywhere else
 *
 *     [timeBefore]	same as everywhere else
 *
 *     [timeAfter]	same as everywhere else
 */
DatasourceFile.prototype.scanInit = function (args)
{
	var error, path, findstream, parser, predicate, filterstream;

	mod_assertplus.object(args, 'args');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.optionalObject(args.filter, 'args.filter');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	error = null;
	if (this.ds_timefield === null &&
	    (args.timeBefore !== null || args.timeAfter !== null)) {
		error = new VError('datasource is missing "timefield" ' +
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

	if (args.filter !== null) {
		predicate = mod_krill.createPredicate(args.filter);
		filterstream = new KrillSkinnerStream(predicate,
		    'Datasource filter');
		filterstream = new PipelineStream({
		    'streams': [ parser, filterstream ],
		    'streamOptions': {
			'objectMode': true
		    }
		});
	} else {
		filterstream = parser;
	}

	mod_assertplus.string(this.ds_datapath);
	path = this.ds_datapath;
	if (this.ds_timeformat !== null) {
		findstream = this.findStream(path, this.ds_timeformat,
		    args.timeAfter || null, args.timeBefore || null);
	} else {
		if (args.timeBefore !== null || args.timeAfter !== null) {
			console.error('warn: datasource is missing ' +
			    '"timeformat" for "before" and "after" ' +
			    'constraints');
		}

		findstream = this.findStream(path, null, null, null);
	}
	if (findstream instanceof Error)
		return (findstream);

	if (args.dryRun)
		return (this.dryRun(findstream));

	return ({
	    'parser': filterstream,
	    'findstream': findstream
	});
};

/*
 * Execute a dry-run scan, index, or query.  Given a findstream that will emit
 * files to scan, print out the files.  Returns an object with "outstream",
 * which is a stream that emits "end" when all files have been printed out.
 */
DatasourceFile.prototype.dryRun = function (findstream)
{
	var rv;

	rv = mod_vstream.wrapStream(new mod_stream.PassThrough());
	console.error('would scan files:');
	findstream.on('data', function (fileinfo) {
		if (fileinfo.error)
			return;
		console.error('    %s', fileinfo.path);
	});
	findstream.on('end', function () { rv.end(); });
	return ({ 'outstream': rv });
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

		mod_assertplus.ok(fileinfo.stat.isFile() ||
		    fileinfo.stat.isCharacterDevice());
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
	var scanargs, key;

	mod_assertplus.ok(!args.hasOwnProperty('sink'));
	mod_assertplus.ok(!args.hasOwnProperty('filter'));

	scanargs = {};
	for (key in args)
		scanargs[key] = args[key];
	scanargs.filter = this.ds_filter;

	return (this.indexScanImpl(scanargs, callback));
};

DatasourceFile.prototype.indexScanImpl = function (args, callback)
{
	var error, scanctx, metrics, datastream, queries, barrier, sink, i;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.arrayOfObject(args.metrics, 'args.metrics');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.string(args.interval, 'args.interval');
	mod_assertplus.optionalObject(args.filter, 'args.filter');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	error = this.checkTimeArgs(args);
	if (error === null)
		error = this.checkIndexArgs(args, args.sink === undefined,
		    true);

	if (error === null) {
		scanctx = this.scanInit({
		    'filter': args.filter || null,
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
		scanctx.outstream.on('data', function () {});
		return (scanctx.outstream);
	}

	/*
	 * Construct the sink, which will read skinner-style data points and
	 * write them into a sqlite index.
	 */
	metrics = args.metrics;
	if (args.sink) {
		sink = args.sink;
	} else {
		sink = this.indexSink({
		    'metrics': metrics,
		    'interval': args.interval
		});
		if (sink instanceof Error) {
			setImmediate(callback, sink);
			return (null);
		}
	}

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
		    args.timeAfter, args.timeBefore, args.interval,
		    self.ds_timefield));
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
		var streamargs, s, t;

		streamargs = {
		    'query': q,
		    'timeField': self.ds_timefield,
		    'log': self.ds_log.child({ 'metric': qi })
		};
		s = new StreamScan(streamargs);
		t = mod_vstream.wrapStream(mod_streamutil.transformStream({
		    'streamOptions': { 'objectMode': true, 'highWaterMark': 0 },
		    'func': function (chunk, _, xformcb) {
			chunk['fields']['__dn_metric'] = qi;
			this.push(chunk);
			setImmediate(xformcb);
		    }
		}), 'Add __dn_metric');

		scanctx.parser.pipe(s);
		s.pipe(t);
		t.pipe(sink, { 'end': false });

		barrier.start('query ' + qi);
		t.on('end', function () { barrier.done('query ' + qi); });
	});

	sink.on('flushed', callback);
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
	var barrier, sink, multiplexer;
	var prefixlen, suffix;
	var interval, root;
	var self = this;

	interval = args.interval;
	if (interval == 'all') {
		sink = new IndexSink({
		    'log': this.ds_log,
		    'metrics': args.metrics,
		    'filename': mod_path.join(this.ds_indexpath, 'all')
		});

		sink.on('finish', function () { sink.emit('flushed'); });
		return (sink);
	}

	switch (interval) {
	case 'hour':
		prefixlen = '2014-07-02T00'.length;
		suffix = ':00:00Z';
		break;

	case 'day':
		prefixlen = '2014-07-02'.length;
		suffix = 'T00:00:00Z';
		break;

	default:
		return (new VError('unsupported interval: "%s"', interval));
	}

	/*
	 * When generating an "hourly" or "daily" Dragnet index, we really
	 * generate a group of per-hour or per-day index files.  Each of these
	 * files is generated by an IndexSink stream, which accepts aggregated
	 * data points (as emitted by either the scanner or querier) and writes
	 * them into a sqlite index file.
	 *
	 * The aggregated data points may be emitted in any order.  A
	 * Multiplexer is responsible for looking at the timestamp in each data
	 * point and deciding which IndexSink stream that point should be
	 * written to.  IndexSinks (and the corresponding index files) are
	 * created only as needed.
	 */
	root = mod_path.join(this.ds_indexpath, 'by_' + interval);
	barrier = mod_vasync.barrier();
	multiplexer = new MultiplexStream({
	    'log': self.ds_log.child({ 'component': 'multiplexer' }),

	    'streamOptions': { 'highWaterMark': 0 },

	    'bucketer': function (record) {
		var dnts, tsms, tsdate, datestr, bucketname, bucketstart;

		dnts = record['fields']['__dn_ts'];
		mod_assertplus.equal(typeof (dnts), 'number');
		mod_assertplus.ok(!isNaN(dnts));
		tsms = dnts * 1000;
		tsdate = new Date(tsms);
		datestr = tsdate.toISOString();
		bucketname = datestr.substr(0, prefixlen);
		bucketstart = Date.parse(bucketname + suffix) / 1000;
		return ({
		    'name': bucketname,
		    'timestamp': tsdate,
		    'start': bucketstart
		});
	    },

	    'bucketCreate': function (_, bucketdesc) {
		var label, indexpath, indexsink;

		barrier.start(bucketdesc.name);
		label = bucketdesc.name.replace(/T/, '-');
		indexpath = mod_path.join(root, label + '.sqlite');
		indexsink = new IndexSink({
		    'log': self.ds_log.child({ 'indexer': label }),
		    'metrics': args.metrics,
		    'filename': indexpath,
		    'config': {
			'dn_start': bucketdesc.start
		    }
		});
		indexsink.on('error', function (err) {
			barrier.done(bucketdesc.name);
			multiplexer.emit('error',
			    new VError(err, 'index "%s"', label));
		});
		indexsink.on('finish', function () {
			barrier.done(bucketdesc.name);
		});
		multiplexer.vsRecordPipe(indexsink);
		return (indexsink);
	    }
	});
	barrier.start('multiplexer');
	multiplexer = mod_vstream.wrapStream(multiplexer, 'multiplexer');
	multiplexer.on('finish', function () { barrier.done('multiplexer'); });
	barrier.on('drain', function () { multiplexer.emit('flushed'); });
	return (multiplexer);
};

DatasourceFile.prototype.checkTimeArgs = function (args)
{
	if (args.timeAfter !== null && args.timeBefore === null)
		return (new VError('cannot specify --after without --before'));
	else if (args.timeBefore !== null && args.timeAfter === null)
		return (new VError('cannot specify --before without --after'));
	return (null);
};

DatasourceFile.prototype.checkIndexArgs = function (args, needsindex,
    needstime)
{
	if (needsindex && this.ds_indexpath === null)
		return (new VError('datasource is missing "indexpath"'));
	if (needstime && args.interval != 'all' && this.ds_timefield === null)
		return (new VError('datasource is missing "timefield"'));
	return (null);
};

/*
 * [public] Query a set of indexes.  Arguments are the same as scan(), plus:
 *
 *     interval		force which index to use (same values as for build())
 */
DatasourceFile.prototype.query = function (args)
{
	var error, findparams, findstream, querystream, aggr;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	error = this.checkTimeArgs(args);
	if (error === null)
		error = this.checkIndexArgs(args, true, false);

	findparams = mod_dragnet_impl.indexFindParams({
	    'interval': args.interval || 'all',
	    'indexpath': this.ds_indexpath,
	    'timeAfter': args.query.qc_after,
	    'timeBefore': args.query.qc_before
	});
	if (findparams instanceof Error)
		error = findparams;

	if (error === null) {
		findstream = this.findStream(findparams.root,
		    findparams.timeformat, findparams.after, findparams.before);
		if (findstream instanceof Error)
			error = findstream;
	}

	if (error !== null)
		return (mod_dragnet_impl.asyncError(error));

	if (args.dryRun)
		return (this.dryRun(findstream).outstream);

	querystream = this.queryStream(findstream, args.query);
	aggr = mod_dragnet_impl.queryAggrStream({
	    'query': args.query,
	    'options': { 'resultsAsPoints': true }
	});
	aggr = mod_vstream.wrapTransform(aggr, 'Index Result Aggregator');
	querystream.on('error', aggr.emit.bind(aggr, 'error'));
	querystream.pipe(aggr);
	return (aggr);
};

/*
 * Given a FindStream emitting index filenames, attach a pipeline that will load
 * the indexes, run the query, and emit data points.
 */
DatasourceFile.prototype.queryStream = function (findstream, query)
{
	var barrier, rv;
	var self = this;

	barrier = mod_vasync.barrier();
	barrier.start('find');

	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 128
	});
	rv.setMaxListeners(Infinity);
	rv = mod_vstream.wrapTransform(rv, 'Index List');
	findstream.vsRecordPipe(rv);

	/*
	 * XXX Consider making this an object-mode transformer for flow-control.
	 */
	findstream.on('data', function (fileinfo) {
		var queryindex;

		if (fileinfo.error) {
			rv.emit('find_error', fileinfo);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile() ||
		    fileinfo.stat.isCharacterDevice());
		barrier.start(fileinfo.path);
		queryindex = new IndexQuerier({
		    'log': self.ds_log.child({ 'queryindex': fileinfo.path }),
		    'filename': fileinfo.path
		});

		queryindex.on('error', function (err) {
			err = new VError(err, 'index "%s"', fileinfo.path);
			rv.emit('error', err);
		});

		queryindex.on('ready', function () {
			/*
			 * XXX Should this be in a vasync queue to manage
			 * concurrency?
			 */
			var qrun;

			qrun = queryindex.run(query);
			qrun.pipe(rv, { 'end': false });
			qrun.on('end', function () {
				barrier.done(fileinfo.path);
			});
			qrun.on('error', function (err) {
				err = new VError(err, 'index "%s" query',
				    fileinfo.path);
				rv.emit('error', err);
				barrier.done(fileinfo.path);
			});
		});
	});

	findstream.on('end', function () {
		barrier.done('find');
	});

	barrier.on('drain', function () { rv.end(); });
	return (rv);
};

/*
 * [public] Like build(), in that we run all the input data through a separate
 * StreamScan for each metric, but emit the results as points rather than into a
 * group of sqlite files.
 */
DatasourceFile.prototype.indexScan = function (args)
{
	var scanargs, sink;

	sink = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 128
	});

	scanargs = {
	    'dryRun': false,
	    'filter': args.filter, /* datasource filter */
	    'interval': args.interval,
	    'metrics': args.metrics,
	    'sink': sink,
	    'timeAfter': args.timeAfter,
	    'timeBefore': args.timeBefore
	};

	this.indexScanImpl(scanargs, function (err) {
		if (err)
			sink.emit('error', err);
	});

	return (sink);
};

/*
 * [public] The counter to indexScan, this reads a bunch of data points and
 * creates index files for the data contained therein.
 */
DatasourceFile.prototype.indexRead = function (args, callback)
{
	var error, sink, parser;

	error = null;
	sink = this.indexSink(args);
	if (sink instanceof Error) {
		setImmediate(callback, error);
		return (null);
	}

	parser = mod_dragnet_impl.parserFor('json-skinner');
	mod_assertplus.ok(!(parser instanceof Error));
	process.stdin.pipe(parser); /* XXX parametrize? */
	parser.pipe(sink);
	sink.on('flushed', callback);
	return (sink);
};
