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
exports.JsonLineStream = JsonLineStream;
exports.SkinnerReadStream = SkinnerReadStream;

/*
 * JsonLineStream
 *
 *     input:  byte mode (buffers or strings)	newline-separated JSON objects
 *
 *     output: object-mode			skinner-format objects
 */
function JsonLineStream(opts)
{
	var streamoptions;

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true });
	this.jr_lstream = mod_vstream.wrapStream(
	    new mod_lstream(streamoptions));
	this.jr_jsondecoder = new JsonDecoderStream();
	this.jr_adapter = new SkinnerAdapterStream();

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	PipelineStream.call(this, {
	    'streams': [
	        this.jr_lstream, this.jr_jsondecoder, this.jr_adapter ],
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(JsonLineStream, PipelineStream);

/*
 * SkinnerReadStream
 *
 *    input:	byte-mode (buffers or strings), newline-separated skinner data
 *
 *    output:	object-mode, skinner-format objects
 */
function SkinnerReadStream(opts)
{
	var streamoptions;

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true });
	this.sr_lstream = mod_vstream.wrapStream(
	    new mod_lstream(streamoptions));
	this.sr_jsondecoder = new JsonDecoderStream();

	streamoptions = mod_streamutil.streamOptions(opts,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	PipelineStream.call(this, {
	    'streams': [ this.sr_lstream, this.sr_jsondecoder ],
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(SkinnerReadStream, PipelineStream);

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

/*
 * SkinnerAdapterStream
 *
 *     input	object-mode: plain JavaScript objects
 *
 *     output	object-mode: skinner-format objects
 */
function SkinnerAdapterStream()
{
	mod_stream.Transform.call(this, {
	    'objectMode': true,
	    'highWaterMark': 0
	});
	mod_vstream.wrapTransform(this);
}

mod_util.inherits(SkinnerAdapterStream, mod_stream.Transform);

SkinnerAdapterStream.prototype._transform = function (chunk, _, callback)
{
	this.push({ 'fields': chunk, 'value': 1 });
	setImmediate(callback);
};
