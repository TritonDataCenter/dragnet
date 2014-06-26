/*
 * lib/source-fileset.js: data source backed by a set of files structured under
 * a directory tree.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_vasync = require('vasync');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_dragnet_impl = require('./dragnet-impl');
var mod_pathenum = require('./path-enum');
var FindStream = require('./fs-find');

var CatStreams = require('catstreams');
var IndexSink = require('./index-sink');
var MultiplexStream = require('./stream-multiplex');
var QueryIndex = require('./query-index');
var StreamScan = require('./stream-scan');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

module.exports = FileSetDataSource;

function FileSetDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.dataroot, 'args.dataroot');

	this.fss_log = args.log;
	this.fss_dataroot = args.dataroot;
}

/*
 * "scan" operation arguments:
 *
 *     query		a QueryConfig object
 *
 *     format		string name of file format
 *
 *     timeField	name of the input field containing the timestamp.
 *     			see StreamScan.
 *
 *     [timeFormat]	string describing how data files are organized.
 *     			If not specified, all files will be scanned for all
 *     			operations, even if timeBefore and timeAfter are set.
 *
 *     [timeBefore]	Date denoting the earliest time to scan  "timeFormat"
 *     			must also be specified.
 *
 *     [timeAfter]	Date denoting the latest time to scan.  "timeFormat"
 *     			must also be specified.
 *
 * XXX make timeBefore and timeAfter part of the query?
 * XXX some of this could probably be commonized with the FileDataSource.
 */
FileSetDataSource.prototype.scan = function (args)
{
	var path, rv, finder, stream, catstream, parser, scan;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.timeField, 'args.timeField');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');

	if (args.timeBefore !== undefined || args.timeAfter !== undefined)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	if (args.timeBefore !== undefined)
		mod_assertplus.object(args.timeAfter, 'args.timeAfter');
	if (args.timeAfter !== undefined)
		mod_assertplus.object(args.timeBefore, 'args.timeBefore');

	/*
	 * Instantiate the pipeline: file ReadStream, format parser, and
	 * scanner.  The ReadStream can emit an error, but the others generally
	 * won't; instead, they emit "invalid_object" for particular records
	 * that are malformed.  We proxy recoverable errors and these
	 * "invalid_object" events to our caller.
	 */
	path = this.fss_dataroot;
	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error) {
		rv = new mod_stream.PassThrough();
		setImmediate(function () { rv.emit('error', parser); });
		return (rv);
	}

	finder = new FindStream({
	    'log': this.fss_log.child({ 'component': 'find' })
	});

	if (args.timeBefore !== undefined) {
		stream = mod_pathenum.createPathEnumerator({
		    'pattern': mod_path.join(path, args.timeFormat),
		    'timeStart': args.timeAfter,
		    'timeEnd': args.timeBefore
		});
		if (stream instanceof Error) {
			rv = new mod_stream.PassThrough();
			setImmediate(function () { rv.emit('error', stream); });
			return (rv);
		}
		stream.pipe(finder);
	} else {
		finder.write(path);
		finder.end();
	}

	/*
	 * XXX catstreams should really be writable so that we can pipe this and
	 * get flow control.  For now, we may end up buffering the names of all
	 * files found.
	 */
	catstream = new CatStreams({
	    'log': this.fss_log,
	    'perRequestBuffer': 16384,
	    'maxConcurrency': 2
	});

	scan = new StreamScan({
	    'query': args.query,
	    'timeField': args.timeField,
	    'log': this.fss_log.child({ 'component': 'scan' })
	});

	finder.on('data', function (fileinfo) {
		if (fileinfo.error) {
			/* XXX */
			console.error('warn: "%s": %s"', fileinfo.path,
			    fileinfo.error.message);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile());
		catstream.cat(function () {
			return (mod_fs.createReadStream(fileinfo.path));
		});
	});

	finder.on('end', function () {
		catstream.cat(null);
	});

	finder.on('error', function (err) {
		scan.emit('error', new VError(err, 'find'));
	});

	parser.on('invalid_record', scan.emit.bind(scan, 'invalid_record'));

	catstream.pipe(parser);
	parser.pipe(scan);

	/* Hang a few fields off for debugging. */
	scan.fss_dataroot = path;
	return (scan);
};

/*
 * "index" operation arguments:
 *
 *     index		an IndexConfig object
 *
 *     indexroot	string path to where indexes should go
 *
 *     format		string name of file format
 *
 *     timeField	name of field to parse as time field
 */
FileSetDataSource.prototype.index = function (args)
{
	var index, query, scan, multiplexer, root, columns;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.string(args.timeField, 'args.timeField');

	/*
	 * Generate a query whose results will match the contents of the
	 * index.
	 * XXX index resolution must be compatible with the index chunking
	 * resolution.
	 */
	index = args.index;
	root = args.indexroot;
	columns = index.ic_columns.slice(0);
	/*
	 * XXX think through how we do this.  should the name of this field
	 * *replace* the user's timestamp field, if we find it?  what if the
	 * user also wants to quantize on this?
	 */
	columns.push({
	    'name': '__dn_ts',
	    'field': '__dn_ts',
	    'aggr': 'lquantize',
	    'step': index.ic_resolution
	});
	query = mod_dragnet.queryLoad({
	    'query': {
		'filter': index.ic_filter,
		'breakdowns': columns
	    }
	});
	mod_assertplus.ok(!(query instanceof Error));

	/*
	 * Run a scan.  We'll pipe the outputs to an Index sink.
	 */
	scan = this.scan({
	    'query': query,
	    'format': args.format,
	    'timeField': args.timeField
	});

	if (scan instanceof mod_stream.PassThrough)
		/* XXX */
		return (scan);

	multiplexer = new MultiplexStream({
	    'log': self.fss_log.child({ 'component': 'multiplexer' }),

	    'streamOptions': {
		'highWaterMark': 0
	    },

	    'bucketer': function (record) {
		var dnts, tsms, tsdate, bucketname;

		dnts = record['fields']['__dn_ts'];
		mod_assertplus.equal(typeof (dnts), 'number');
		mod_assertplus.ok(!isNaN(dnts));
		tsms = dnts * 1000;
		tsdate = new Date(tsms);
		bucketname = tsdate.toISOString().substr(0, 13);

		return ({
		    'name': bucketname,
		    'timestamp': tsdate
		});
	    },

	    'bucketCreate': function (_, bucketdesc) {
		var t, label, indexpath, indexsink;

		t = bucketdesc.timestamp;
		label = sprintf('%s-%02d-%02d-%02d',
		    t.getUTCFullYear(), t.getUTCMonth() + 1,
		    t.getUTCDate(), t.getUTCHours());
		indexpath = mod_path.join(root, label + '.sqlite');
		indexsink = new IndexSink({
		    'log': self.fss_log.child({ 'indexer': label }),
		    'index': index,
		    'filename': indexpath
		});
		indexsink.on('error', function (err) {
			multiplexer.emit('error',
			    new VError(err, 'index "%s"', label));
		});
		return (indexsink);
	    }
	});

	scan.pipe(multiplexer);
	scan.on('error', multiplexer.emit.bind('error'));
	scan.on('invalid_object', multiplexer.emit.bind('invalid_object'));
	scan.on('invalid_record', multiplexer.emit.bind('invalid_record'));
	/* XXX */
	multiplexer.stats = function () { return (scan.stats()); };
	/*
	 * XXX this is the wrong stream to return in that when the caller gets
	 * "finish", that doesn't mean we've flushed the indexers.
	 */
	return (multiplexer);
};


/*
 * "query" operation arguments:
 *
 *    query		a QueryConfig object
 *
 *    indexroot		name of index tree to search
 *
 *    [timeBefore]	Date denoting the earliest time to scan  "timeFormat"
 *    			must also be specified.
 *
 *    [timeAfter]	Date denoting the latest time to scan.  "timeFormat"
 *     			must also be specified.
 *
 * XXX make timeBefore and timeAfter part of the query?
 */
FileSetDataSource.prototype.query = function (args)
{
	var query, path, finder, stream, aggr, barrier;
	var self = this;

	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');

	if (args.timeBefore !== undefined)
		mod_assertplus.object(args.timeAfter, 'args.timeAfter');
	if (args.timeAfter !== undefined)
		mod_assertplus.object(args.timeBefore, 'args.timeBefore');

	query = args.query;
	path = args.indexroot;
	finder = new FindStream({
	    'log': this.fss_log.child({ 'component': 'find' })
	});
	barrier = mod_vasync.barrier();
	aggr = mod_dragnet_impl.queryAggrStream({
	    'query': query,
	    'options': { 'resultsAsPoints': true }
	});
	aggr.setMaxListeners(Infinity);
	barrier.start('find');

	if (args.timeBefore) {
		stream = mod_pathenum.createPathEnumerator({
		    'pattern': mod_path.join(path, '%Y-%m-%d-%H.sqlite'),
		    'timeStart': args.timeAfter,
		    'timeEnd': args.timeBefore
		});

		if (stream instanceof Error) {
			var rv = new mod_stream.PassThrough();
			setImmediate(function () { rv.emit('error', stream); });
			return (rv);
		}

		stream.pipe(finder);
	} else {
		finder.write(path);
		finder.end();
	}

	/*
	 * XXX Consider making this an object-mode transformer for flow-control.
	 */
	finder.on('data', function (fileinfo) {
		var queryindex;

		if (fileinfo.error) {
			/* XXX */
			console.error('warn: "%s": %s"', fileinfo.path,
			    fileinfo.error.message);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile());
		barrier.start(fileinfo.path);
		queryindex = new QueryIndex({
		    'log': self.fss_log.child({ 'queryindex': fileinfo.path }),
		    'filename': fileinfo.path
		});

		queryindex.on('error', function (err) {
			err = new VError(err, 'index "%s"', fileinfo.path);
			aggr.emit('error', err);
		});

		queryindex.on('ready', function () {
			/*
			 * XXX Should this be in a vasync queue to manage
			 * concurrency?
			 * XXX Does it work to pipe multiple streams into one
			 * stream?
			 */
			var qrun = queryindex.run(query);
			qrun.pipe(aggr, { 'end': false });
			qrun.on('end', function () {
				barrier.done(fileinfo.path);
			});
			qrun.on('error', function (err) {
				err = new VError(err, 'index "%s" query',
				    fileinfo.path);
				aggr.emit('error', err);
				barrier.done(fileinfo.path);
			});
		});
	});

	finder.on('error', function (err) {
		aggr.emit('error', new VError(err, 'find'));
	});

	finder.on('end', function () {
		barrier.done('find');
	});

	barrier.on('drain', function () {
		aggr.end();
	});

	return (aggr);
};
