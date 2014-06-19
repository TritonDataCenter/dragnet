/*
 * lib/source-fileset.js: data source backed by a set of files structured under
 * a directory tree.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_stream = require('stream');

var mod_dragnet_impl = require('./dragnet-impl');
var mod_find = require('./fs-find');

var CatStreams = require('catstreams');
var StreamScan = require('./stream-scan');
var VError = require('verror');

module.exports = FileSetDataSource;

function FileSetDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.path, 'args.path');

	this.fss_log = args.log;
	this.fss_path = args.path;
}

/*
 * "scan" operation arguments:
 *
 *     query		a QueryConfig object
 *
 *     format		string name of file format
 */
FileSetDataSource.prototype.scan = function (args)
{
	var path, rv, finder, catstream, parser, scan;

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
	path = this.fds_path;
	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error) {
		rv = new mod_stream.PassThrough();
		setImmediate(function () { rv.emit('error', parser); });
		return (rv);
	}

	finder = mod_find({ 'path': this.fss_path });

	catstream = new CatStreams({
	    'log': this.fss_log,
	    'perRequestBuffer': 16384,
	    'maxConcurrency': 2
	});

	scan = new StreamScan({
	    'query': args.query,
	    'log': this.fss_log.child({ 'component': 'scan' })
	});

	finder.on('entry', function (filepath, st) {
		if (!st.isFile())
			return;

		catstream.cat(function () {
			return (mod_fs.createReadStream(filepath));
		});
	});

	finder.on('end', function () {
		catstream.cat(null);
	});

	finder.on('error', function (err) {
		scan.emit('error', new VError(err, 'find "%s"', path));
	});

	parser.on('invalid_record', scan.emit.bind(scan, 'invalid_record'));

	catstream.pipe(parser);
	parser.pipe(scan);

	/* Hang a few fields off for debugging. */
	scan.fss_path = path;
	return (scan);
};
