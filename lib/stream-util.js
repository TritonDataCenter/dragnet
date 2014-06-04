/*
 * lib/stream-util.js: miscellaneous stream utilites
 */

var VError = require('verror');

/* Public interface */
exports.readToEndString = readToEndString;
exports.readToEndJson = readToEndJson;

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
