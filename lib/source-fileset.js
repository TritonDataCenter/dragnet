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
 *     [timeFormat]	string describing how data files are organized.
 *     			Must be specified if timeBefore and timeAfter are
 *     			specified with the query.
 *
 * XXX some of this could probably be commonized with the FileDataSource.
 */
FileSetDataSource.prototype.scan = function (args)
{
	var rv, err;

	rv = this.scanImpl(args);
	if (rv instanceof Error) {
		err = rv;
		rv = new mod_stream.PassThrough();
		setImmediate(rv.emit.bind(rv, 'error', err));
	}

	return (rv);
};

/*
 * Behaves just like scan(), but may return an Error rather than a stream.
 * This exists separately because the real user-facing interface emits
 * validation errors asynchronously, but its useful for the internal interface
 * to be able to distinguish such errors synchronously.
 */
FileSetDataSource.prototype.scanImpl = function (args)
{
	var path, finder, stream, catstream, parser, scan, rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');

	if (args.query.qc_before !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	/*
	 * Instantiate the pipeline: file ReadStream, format parser, and
	 * scanner.  The ReadStream can emit an error, but the others generally
	 * won't; instead, they emit "invalid_object" for particular records
	 * that are malformed.  We proxy recoverable errors and these
	 * "invalid_object" events to our caller.
	 */
	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	});
	path = this.fss_dataroot;
	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error)
		return (parser);

	finder = new FindStream({
	    'log': this.fss_log.child({ 'component': 'find' })
	});

	if (args.query.qc_before !== null) {
		stream = mod_pathenum.createPathEnumerator({
		    'pattern': mod_path.join(path, args.timeFormat),
		    'timeStart': args.query.qc_after,
		    'timeEnd': args.query.qc_before
		});
		if (stream instanceof Error)
			return (stream);
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
	    'log': this.fss_log.child({ 'component': 'scan' })
	});

	finder.on('data', function (fileinfo) {
		if (fileinfo.error) {
			rv.emit('find_error', fileinfo);
			return;
		}

		rv.fss_nfiles++;
		mod_assertplus.ok(fileinfo.stat.isFile());
		catstream.cat(function () {
			return (mod_fs.createReadStream(fileinfo.path));
		});
	});

	finder.on('end', function () {
		catstream.cat(null);
	});

	finder.on('error', function (err) {
		rv.emit('error', new VError(err, 'find'));
	});

	parser.on('invalid_record', rv.emit.bind(rv, 'invalid_record'));

	catstream.pipe(parser);
	parser.pipe(scan);
	scan.pipe(rv);

	/* Hang a few fields off for debugging. */
	rv.fss_nfiles = 0;
	rv.fss_dataroot = path;
	rv.stats = function () {
		var st = scan.stats();
		st['nscanned'] = this.fss_nfiles;
		return (st);
	};
	return (rv);
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
 *     [source]		'raw' (default) means to scan raw data;
 *     			'hour' means to query hourly indexes, and so on.
 *
 *     [timeFormat]	see scan().
 */
FileSetDataSource.prototype.index = function (args)
{
	var index, query, scan, multiplexer, root, columns, step;
	var self = this;
	var rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.string(args.source, 'args.source');

	if (args.index.ic_before !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	if (args.index.ic_timefield === null) {
		rv = new mod_stream.PassThrough();
		setImmediate(rv.emit.bind(rv, 'error',
		    new VError('at least one field must be a "date"')));
		return (rv);
	}

	/*
	 * Generate a query whose results will match the contents of the
	 * index.
	 * XXX index interval must be compatible with the index chunking
	 * interval.
	 */
	index = args.index;
	root = mod_path.join(args.indexroot, 'by_' + index.ic_interval);
	columns = index.ic_columns.slice(0);

	if (index.ic_interval == 'hour') {
		step = 3600;
	} else {
		mod_assertplus.equal(index.ic_interval, 'day');
		step = 86400;
	}

	columns.push({
	    'name': '__dn_ts',
	    'field': index.ic_timefield.field,
	    'date': true,
	    'aggr': 'lquantize',
	    'step': step
	});
	query = mod_dragnet.queryLoad({
	    'allowReserved': true,
	    'query': {
		'filter': index.ic_filter,
		'breakdowns': columns,
		'timeAfter': index.ic_after,
		'timeBefore': index.ic_before
	    }
	});
	mod_assertplus.ok(!(query instanceof Error), query.message);

	/*
	 * Run a scan.  We'll pipe the outputs to an Index sink.
	 */
	if (args.source == 'raw') {
		scan = this.scanImpl({
		    'query': query,
		    'format': args.format,
		    'timeFormat': args.timeFormat
		});
	} else {
		scan = this.query({
		    'query': query,
		    'indexroot': args.indexroot
		});
	}

	if (scan instanceof Error) {
		rv = new mod_stream.PassThrough();
		setImmediate(rv.emit.bind(rv, 'error', scan));
		return (rv);
	}

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
	scan.on('error', multiplexer.emit.bind(multiplexer, 'error'));
	scan.on('find_error', multiplexer.emit.bind(multiplexer, 'find_error'));
	scan.on('invalid_object', multiplexer.emit.bind(multiplexer,
	    'invalid_object'));
	scan.on('invalid_record', multiplexer.emit.bind(multiplexer,
	    'invalid_record'));
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
 */
FileSetDataSource.prototype.query = function (args)
{
	var rv, query, path, finder, stream, aggr, barrier;
	var self = this;

	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.indexroot, 'args.indexroot');

	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	});
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

	if (query.qc_before !== null) {
		stream = mod_pathenum.createPathEnumerator({
		    'pattern': mod_path.join(path, 'by_hour',
		        '%Y-%m-%d-%H.sqlite'),
		    'timeStart': query.qc_after,
		    'timeEnd': query.qc_before
		});

		if (stream instanceof Error) {
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
			rv.emit('find_error', fileinfo);
			return;
		}

		rv.fsq_nfiles++;
		mod_assertplus.ok(fileinfo.stat.isFile());
		barrier.start(fileinfo.path);
		queryindex = new QueryIndex({
		    'log': self.fss_log.child({ 'queryindex': fileinfo.path }),
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
				rv.emit('error', err);
				barrier.done(fileinfo.path);
			});
		});
	});

	finder.on('error', function (err) {
		rv.emit('error', new VError(err, 'find'));
	});

	finder.on('end', function () {
		barrier.done('find');
	});

	barrier.on('drain', function () {
		aggr.end();
	});

	aggr.pipe(rv);
	rv.fsq_nfiles = 0;
	rv.stats = function () {
		var st = aggr.stats();
		st['nscanned'] = this.fsq_nfiles;
		return (st);
	};
	return (rv);
};
