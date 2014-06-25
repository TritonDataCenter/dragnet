/*
 * lib/stream-pipe.js: "pipeline" stream, which is a single stream that pipes
 * input through another pipeline and emits the output.
 */

var mod_assertplus = require('assert-plus');
var mod_stream = require('stream');
var mod_util = require('util');

/* Public interface */
module.exports = PipelineStream;

/*
 * XXX is it important that these be piped here?  is there any useful way you
 * could just give us a *pair* of streams and we treat those as "head" and
 * "tail", and leave the semantics to the caller?
 *
 * streams			pipeline streams, in order
 * (array of streams)
 *
 * streamOptions		options to pass through to the Node.js Stream
 * (object)			constructor
 */
function PipelineStream(args)
{
	var self = this;
	var i;

	mod_assertplus.object(args, 'args');
	mod_assertplus.arrayOfObject(args.streams, 'args.streams');
	mod_assertplus.ok(args.streams.length > 0);
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	mod_stream.Duplex.call(this, args.streamOptions);
	this.ps_needreadable = false;
	this.ps_streams = args.streams.slice(0);
	this.ps_head = this.ps_streams[0];
	this.ps_tail = this.ps_streams[this.ps_streams.length - 1];
	this.ps_tail.on('readable', this.onTailReadable.bind(this));
	this.ps_tail.on('end', function () { self.push(null); });
	this.once('finish', function () { self.ps_head.end(); });

	for (i = 0; i < this.ps_streams.length; i++)
		this.ps_streams[i].on('error', this.emit.bind(this, 'error'));

	for (i = 0; i < this.ps_streams.length - 1; i++)
		this.ps_streams[i].pipe(this.ps_streams[i + 1]);
}

mod_util.inherits(PipelineStream, mod_stream.Duplex);

PipelineStream.prototype._write = function (chunk, encoding, callback)
{
	this.ps_head.write(chunk, encoding, callback);
};

PipelineStream.prototype._read = function ()
{
	var chunk;

	/*
	 * Node itself should avoid calling _read() if there's no place to put
	 * the resulting data.  We only call _read() when the tail has become
	 * readable while we were waiting for that condition.  So we should
	 * never be called when our outgoing buffer is full.  We assert that
	 * here because violating this condition can (and has) lead to
	 * potentially unbounded memory leakage.  It's unfortunate that this
	 * requires reaching into Node-private state, but the value of this
	 * check is worth the dirtiness, and if that becomes untenable because
	 * Node changes, we can always rip this out.
	 */
	if (this._readableState.length > this._readableState.highWaterMark + 1)
		throw (new Error('unexpected call to pipeline._read()'));

	for (;;) {
		chunk = this.ps_tail.read(1);
		if (chunk === null) {
			this.ps_needreadable = true;
			break;
		}

		if (!this.push(chunk))
			break;
	}
};

PipelineStream.prototype.onTailReadable = function ()
{
	if (!this.ps_needreadable)
		return;

	this.ps_needreadable = false;
	this._read();
};
