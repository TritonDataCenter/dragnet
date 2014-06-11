/*
 * lib/format-json.js: JSON stream reader
 */

var mod_assert = require('assert');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_lstream = require('lstream');

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
	var streamoptions, k;

	this.jr_lstream = new mod_lstream();
	this.jr_jsondecoder = new JsonDecoderStream();
	this.jr_jsondecoder.on('invalid_record',
	    this.emit.bind(this, 'invalid_record'));

	streamoptions = {};
	if (opts) {
		for (k in opts)
			streamoptions[k] = opts[k];
	}
	streamoptions['objectMode'] = true;
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
	var streamoptions, k;

	streamoptions = {};
	if (opts) {
		for (k in opts)
			streamoptions[k] = opts[k];
	}
	streamoptions['objectMode'] = true;
	mod_stream.Transform.call(this, streamoptions);

	this.jds_count = 0;
}

mod_util.inherits(JsonDecoderStream, mod_stream.Transform);

JsonDecoderStream.prototype._transform = function (str, encoding, callback)
{
	var obj;

	this.jds_count++;
	mod_assert.equal('string', typeof (str));

	try {
		obj = JSON.parse(str);
	} catch (ex) {
		this.emit('invalid_record', str, ex, this.jds_count);
		callback();
		return;
	}

	this.push(obj);
	callback();
};
