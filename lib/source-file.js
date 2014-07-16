/*
 * lib/source-file.js: data source backed by a single file.  This is useful for
 * testing the basic pieces of the data processing pipeline.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_stream = require('stream');
var mod_vstream = require('./vstream/vstream');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_dragnet_impl = require('./dragnet-impl');
var IndexSink = require('./index-sink');
var QueryIndex = require('../lib/index-query');
var StreamScan = require('./stream-scan');
var VError = require('verror');

module.exports = FileDataSource;

function FileDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.optionalString(args.filename, 'args.filename');

	this.fds_log = args.log;
	this.fds_filename = args.filename || null;
}

/*
 * See FileSetDataSource.asyncError().
 */
FileDataSource.prototype.asyncError = function (err)
{
	var rv = new mod_stream.PassThrough();
	setImmediate(rv.emit.bind(rv, 'error', err));
	return (rv);
};

/*
 * "scan" operation arguments:
 *
 *     query		a QueryConfig object
 *
 *     format		string name of file format
 */
FileDataSource.prototype.scan = function (args)
{
	var filename, fstream, parser, scan;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');

	/*
	 * Instantiate the pipeline: file ReadStream, format parser, and
	 * scanner.  Serious errors and warnings are propagated by the vstream
	 * framework to the caller.
	 */
	mod_assertplus.ok(this.fds_filename !== null);
	filename = this.fds_filename;
	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error)
		return (this.asyncError(parser));

	fstream = mod_fs.createReadStream(filename);
	scan = new StreamScan({
	    'query': args.query,
	    'log': this.fds_log.child({ 'component': 'scan' })
	});

	fstream.pipe(parser);
	parser.pipe(scan);

	/* Hang a few fields off for debugging. */
	scan.fss_filename = filename;
	return (scan);
};

/*
 * "index" operation arguments:
 *
 *     index		an IndexConfig object
 *
 *     filename		name of the index to create
 *
 *     format		string name of file format
 */
FileDataSource.prototype.index = function (args)
{
	var query, scan, sink;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.filename, 'args.filename');

	/*
	 * Generate a query whose results will match the contents of the
	 * index.
	 */
	query = mod_dragnet.queryLoad({
	    'query': {
		'filter': args.index.ic_filter,
		'breakdowns': args.index.ic_columns
	    }
	});
	mod_assertplus.ok(!(query instanceof Error));

	/*
	 * Run a scan.  We'll pipe the outputs to an Index sink.
	 */
	scan = this.scan({
	    'query': query,
	    'format': args.format
	});

	if (scan instanceof mod_stream.PassThrough)
		/* XXX abstract scanImpl and use asyncError */
		return (scan);

	sink = new IndexSink({
	    'log': this.fds_log.child({ 'component': 'indexer' }),
	    'index': args.index,
	    'filename': args.filename
	});
	sink.on('finish', function () { sink.emit('flushed'); });

	scan.pipe(sink);
	return (sink);
};

/*
 * "query" operation arguments:
 *
 *    query		a QueryConfig object
 *
 *    filename		name of index file to search
 */
FileDataSource.prototype.query = function (args)
{
	var rv, filename, query, queryindex;

	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.filename, 'args.filename');

	/*
	 * The buffer size for this pass-through should be enough to absorb
	 * small delays in the consumer, but not so much that we end up using
	 * tons of memory.
	 */
	filename = args.filename;
	query = args.query;
	rv = new mod_stream.PassThrough({
	    'highWaterMark': 512,
	    'objectMode': true
	});
	mod_vstream.wrapTransform(rv, 'Query Passthrough');

	queryindex = new QueryIndex({
	    'log': this.fds_log.child({ 'component': 'queryindex' }),
	    'filename': filename
	});

	queryindex.on('ready', function () {
		var qrun = queryindex.run(query);
		qrun.pipe(rv);
		qrun.on('error', rv.emit.bind(rv, 'error'));
	});

	return (rv);
};
