/*
 * lib/stream-util.js: miscellaneous stream utilites
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_stream = require('stream');
var VError = require('verror');

/* Public interface */
exports.readToEndString = readToEndString;
exports.readToEndJson = readToEndJson;
exports.readFileJson = readFileJson;
exports.streamOptions = streamOptions;
exports.transformStream = transformStream;

/*
 * Read from "stream" to the very end, buffer the entire contents, and convert
 * it to a UTF-8 string.  Invokes "callback" upon completion, either with a
 * stream error or with the complete contents as a string.
 */
function readToEndString(stream, callback)
{
	var data = '';
	stream.on('data', function (c) { data += c.toString('utf8'); });
	stream.on('error', function (err) {
		callback(new VError(err, 'failed to read to end of stream'));
	});
	stream.on('end', function () {
		callback(null, data);
	});
}

/*
 * Like readToEndString(), but also parses the result as JSON.  If that fails,
 * the error is passed to callback.  Otherwise, the plain JavaScript object is
 * passed to callback as the result.
 */
function readToEndJson(stream, callback)
{
	readToEndString(stream, function (err, contents) {
		if (err) {
			callback(err);
			return;
		}

		var json;
		try {
			json = JSON.parse(contents);
		} catch (ex) {
			callback(new VError(ex, 'failed to parse JSON'));
			return;
		}

		callback(null, json);
	});
}

/*
 * Like readToEndJson, but with a filename.  If "schema" is specified, then the
 * file's contents will be validated against the given JSON schema, too.
 */
function readFileJson(args, callback)
{
	var filename, schema, stream;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.filename, 'args.filename');
	mod_assertplus.optionalObject(args.schema, 'args.schema');
	mod_assertplus.func(callback, 'callback');

	filename = args.filename;
	schema = args.schema;
	stream = mod_fs.createReadStream(filename);
	readToEndJson(stream, function (err, obj) {
		if (!err && schema)
			err = mod_jsprim.validateJsonObject(schema, obj);

		if (err)
			callback(new VError(err, 'read "%s"', filename));
		else
			callback(null, obj);
	});
}

/*
 * Given a set of user-passed stream options in "useroptions" (which may be
 * undefined) and a set of overrides (which must be a valid object), construct
 * an options object containing the union, with "overrides" overriding
 * "useroptions", without modifying either object.
 */
function streamOptions(useroptions, overrides, defaults)
{
	return (mod_jsprim.mergeObjects(useroptions, overrides, defaults));
}

/*
 * func			Function to apply for each incoming chunk, as
 * (function)		func(chunk, encoding, callback)
 *
 * [streamOptions]	Options to pass to Node Stream API constructor
 * (object)
 */
function transformStream(args)
{
	var stream;

	mod_assertplus.object(args, 'args');
	mod_assertplus.func(args.func, 'args.func');
	mod_assertplus.optionalFunc(args.flush, 'args.flush');
	mod_assertplus.optionalObject(args.streamOptions);

	stream = new mod_stream.Transform(args.streamOptions);
	stream._transform = args.func;
	if (args.flush)
		stream._flush = args.flush;
	return (stream);
}
