/*
 * lib/format-json.js: JSON stream reader
 */

var mod_assert = require('assert');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_lstream = require('lstream');

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
	var self = this;

	streamoptions = {};
	if (opts) {
		for (k in opts)
			streamoptions[k] = opts[k];
	}
	streamoptions['objectMode'] = true;
	mod_stream.Duplex.call(this, streamoptions);

	this.jr_lstream = new mod_lstream();
	this.jr_lstream.on('error', this.emit.bind(this, 'error'));

	this.jr_jsondecoder = new JsonDecoderStream();
	this.jr_jsondecoder.on('error', this.emit.bind(this, 'error'));
	this.jr_jsondecoder.on('invalid_record',
	    this.emit.bind(this, 'invalid_record'));
	this.jr_jsondecoder.on('readable', this._read.bind(this));

	this.jr_lstream.pipe(this.jr_jsondecoder);
	this.once('finish', function () { self.jr_lstream.end(); });
	this.jr_jsondecoder.on('end', function () { self.push(null); });
}

mod_util.inherits(JsonLineStream, mod_stream.Duplex);

JsonLineStream.prototype._write = function (chunk, encoding, callback)
{
	this.jr_lstream.write(chunk, encoding, callback);
};

JsonLineStream.prototype._read = function ()
{
	var chunk;

	for (;;) {
		chunk = this.jr_jsondecoder.read(1);
		if (chunk === null)
			break;

		if (!this.push(chunk))
			break;
	}
};


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
		this.emit('invalid_record', str, this.jds_count, ex);
		callback();
		return;
	}

	this.push(obj);
	callback();
};
