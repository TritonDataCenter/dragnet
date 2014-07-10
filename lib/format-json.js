/*
 * lib/format-json.js: JSON stream reader
 */

var mod_assert = require('assert');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_lstream = require('lstream');
var mod_streamutil = require('./stream-util');
var mod_vstream = require('./vstream/vstream');

var PipelineStream = require('./stream-pipe');

/* Public interface */
module.exports = JsonLineStream;

/*
 * JsonLineStream
 *
 *     input:  byte mode (buffers or strings)	newline-separated JSON objects
 *
 *     output: object-mode			plain JavaScript objects
 */
function JsonLineStream(opts)
{
	var streamoptions;

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true });

	this.jr_lstream = mod_vstream.wrapStream(
	    new mod_lstream(streamoptions));
	this.jr_jsondecoder = new JsonDecoderStream(streamoptions);

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	PipelineStream.call(this, {
	    'streams': [ this.jr_lstream, this.jr_jsondecoder ],
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(JsonLineStream, PipelineStream);

/*
 * JsonDecoderStream
 *
 *	input:	object mode (strings)	lines of text, 1 JSON object per line
 *
 *	output:	object mode (objects)	plain JavaScript ojects
 */
function JsonDecoderStream(opts)
{
	var streamoptions;

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	mod_stream.Transform.call(this, streamoptions);
	mod_vstream.wrapTransform(this);
}

mod_util.inherits(JsonDecoderStream, mod_stream.Transform);

JsonDecoderStream.prototype._transform = function (str, encoding, callback)
{
	var obj;

	mod_assert.equal('string', typeof (str));

	try {
		obj = JSON.parse(str);
	} catch (ex) {
		this.vsWarn(ex, 'invalid json');
		setImmediate(callback);
		return;
	}

	this.push(obj);
	setImmediate(callback);
};
