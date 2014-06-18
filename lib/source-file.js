/*
 * lib/source-file.js: data source backed by a single file.  This is useful for
 * testing the basic pieces of the data processing pipeline.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_stream = require('stream');

var mod_dragnet_impl = require('./dragnet-impl');
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
 */
FileDataSource.prototype.scan = function (args)
{
	var rv, filename, fstream, parser, scan;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');

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
