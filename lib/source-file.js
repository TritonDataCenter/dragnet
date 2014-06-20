/*
 * lib/source-file.js: data source backed by a single file.  This is useful for
 * testing the basic pieces of the data processing pipeline.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_stream = require('stream');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_dragnet_impl = require('./dragnet-impl');
var IndexSink = require('./index-sink');
var QueryIndex = require('../lib/query-index');
var StreamScan = require('./stream-scan');
var VError = require('verror');

module.exports = FileDataSource;

function FileDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.filename, 'args.filename');

	this.fds_log = args.log;
	this.fds_filename = args.filename;
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
 */
FileDataSource.prototype.scan = function (args)
{
	var rv, filename, fstream, parser, scan;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.optionalString(args.timeField, 'args.timeField');

	/*
	 * Instantiate the pipeline: file ReadStream, format parser, and
	 * scanner.  The ReadStream can emit an error, but the others generally
	 * won't; instead, they emit "invalid_object" for particular records
	 * that are malformed.  We proxy recoverable errors and these
	 * "invalid_object" events to our caller.
	 */
	filename = this.fds_filename;
	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error) {
		rv = new mod_stream.PassThrough();
		setImmediate(function () { rv.emit('error', parser); });
		return (rv);
	}

	fstream = mod_fs.createReadStream(filename);
	scan = new StreamScan({
	    'query': args.query,
	    'timeField': args.timeField,
	    'log': this.fds_log.child({ 'component': 'scan' })
	});

	fstream.on('error', function (err) {
		scan.emit('error', new VError(err, 'read "%s"', filename));
	});
	parser.on('invalid_record', scan.emit.bind(scan, 'invalid_record'));

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
 *
 *     timeField	name of the field providing the timestamp
 */
FileDataSource.prototype.index = function (args)
{
	var query, scan, sink;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.filename, 'args.filename');
	mod_assertplus.string(args.timeField, 'args.timeField');

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
	    'format': args.format,
	    'timeField': args.timeField
	});

	if (scan instanceof mod_stream.PassThrough)
		/* XXX */
		return (scan);

	sink = new IndexSink({
	    'log': this.fds_log.child({ 'component': 'indexer' }),
	    'index': args.index,
	    'filename': args.filename
	});

	scan.pipe(sink);
	scan.on('error', sink.emit.bind('error'));
	scan.on('invalid_object', sink.emit.bind('invalid_object'));
	scan.on('invalid_record', sink.emit.bind('invalid_record'));
	/* XXX */
	sink.stats = function () { return (scan.stats()); };
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

	filename = args.filename;
	query = args.query;
	rv = new mod_stream.PassThrough({ 'objectMode': true });

	queryindex = new QueryIndex({
	    'log': this.fds_log.child({ 'component': 'queryindex' }),
	    'filename': filename
	});

	queryindex.on('error', rv.emit.bind(rv, 'error'));

	queryindex.on('ready', function () {
		var qrun = queryindex.run(query);
		qrun.pipe(rv);
		qrun.on('error', rv.emit.bind(rv, 'error'));
	});

	return (rv);
};
