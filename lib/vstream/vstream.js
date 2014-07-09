/*
 * vstream.js: instrumentable streams mix-ins
 */

var mod_assertplus = require('assert-plus');
var mod_streamutil = require('../stream-util');

var sprintf = require('extsprintf').sprintf;

/* Public interface */
exports.instrumentObject = instrumentObject;
exports.instrumentTransform = instrumentTransform;
exports.instrumentPipelineOps = instrumentPipelineOps;
exports.streamDump = streamDump;
exports.streamDumpPipeline = streamDumpPipeline;
exports.streamHead = streamHead;
exports.streamIter = streamIter;
exports.wrapStream = wrapStream;
exports.wrapTransform = wrapTransform;
exports.wrapTransformHead = wrapTransformHead;
exports.wrapTransformTail = wrapTransformTail;

/*
 * Returns true iff the given object has been initialized for this module.
 */
function isInstrumented(obj)
{
	return (typeof (obj.vs_name) == 'string');
}

/*
 * Given a non-null JavaScript object, initialize the basic properties needed
 * for other components of this module.
 */
function instrumentObject(obj, args)
{
	mod_assertplus.object(obj, 'obj');
	mod_assertplus.ok(!isInstrumented(obj), 'object already instrumented');
	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.name, 'args.name');

	obj.vs_name = args.name;
	obj.vs_counters = {};
	obj.vs_context = null;

	mod_assertplus.ok(!obj.hasOwnProperty('counter'),
	    'cannot instrument object with a "counter" property');
	obj.counter = vcounter;
}

function vcounter(name)
{
	if (!this.vs_counters.hasOwnProperty(name))
		this.vs_counters[name] = 0;
	this.vs_counters[name]++;
}

/*
 * Modify the given object-mode Transform stream to unmarshall incoming data and
 * marshall outgoing data to include provenance information.
 */
function instrumentTransform(transform, options)
{
	var opts;

	opts = mod_streamutil.streamOptions(options, {}, {
	    'unmarshalIn': true,
	    'marshalOut': true
	});

	mod_assertplus.ok(isInstrumented(transform),
	    'transform stream is not an instrumented object');

	transform.vs_ninputs = 0;
	transform.vs_noutputs = 0;
	transform.vs_realtransform = transform._transform;
	transform._transform = vtransform.bind(transform, opts);

	if (opts.marshalOut) {
		transform.vs_realpush = transform.push;
		transform.push = vpush;
	}

	if (transform._flush) {
		transform.vs_realflush = transform._flush;
		transform._flush = vflush.bind(transform, opts);
	}

	transform.pushDrop = vpushdrop;
}

function vtransform(opts, chunk, _, callback)
{
	var self = this;
	var augmented;

	mod_assertplus.ok(isInstrumented(this),
	    'attempted call to vtransform() on uninstrumented Transform');
	mod_assertplus.ok(this.vs_context === null);

	if (!opts.unmarshalIn) {
		mod_assertplus.ok(chunk instanceof ProvenanceValue,
		    'wrote bare value to VTransform ("' +
		    this.vs_name + '") that\'s configured to accept ' +
		    'marshaled objects');
		augmented = chunk;
	} else {
		/*
		 * Technically this is legal, but it's almost certainly not what
		 * the user wanted.
		 */
		mod_assertplus.ok(!(chunk instanceof ProvenanceValue),
		    'wrote marshaled object to VTransform ("' +
		    this.vs_name + '") that\'s configured to accept ' +
		    'bare objects');
		augmented = new ProvenanceValue(chunk);
	}

	this.vs_ninputs++;
	this.vs_context = augmented;
	this.vs_realtransform(augmented.pv_value, _, function (err, newchunk) {
		/*
		 * This check is annoying, but it's the only way we can detect
		 * whether someone's doing something with the API that we don't
		 * know how to handle.  We could likely avoid interpreting the
		 * arguments at all and just pass them straight through to the
		 * original callback, but that callback may end up invoking
		 * another transform without a way for us to know that it
		 * happened, and we'd blow our assertion that vs_context ===
		 * null.  We could also loosen that invariant and just assume
		 * that we're maintaining vs_context correctly, but it's worth
		 * adding a stricter check while it's not too painful.
		 */
		mod_assertplus.ok(arguments.length < 3,
		    '_transform callback passed more arguments than expected');
		mod_assertplus.ok(self.vs_context === augmented);

		if (!err && arguments.length > 1) {
			self.push(newchunk);
			mod_assertplus.ok(self.vs_context === augmented);
		}

		self.vs_context = null;
		callback.apply(null,
		    Array.prototype.slice.call(arguments, 0, 1));
	});
}

function vflush(opts, callback)
{
	var self = this;
	var augmented;

	mod_assertplus.ok(isInstrumented(this),
	    'attempted call to vflush() on uninstrumented Transform');
	mod_assertplus.ok(this.vs_context === null);

	augmented = new ProvenanceValue();
	this.vs_context = augmented;
	this.vs_realflush(function () {
		mod_assertplus.ok(self.vs_context === augmented);
		self.vs_context = null;
		callback.apply(null, Array.prototype.slice.call(arguments));
	});
}

function vpush(chunk)
{
	var augmented;

	mod_assertplus.ok(isInstrumented(this),
	    'attempted call to vpush() on uninstrumented Transform');
	mod_assertplus.ok(chunk === null || this.vs_context !== null);

	if (chunk === null) {
		augmented = null;
	} else {
		this.vs_noutputs++;
		augmented = this.vs_context.next(chunk, this);
	}

	return (this.vs_realpush(augmented));
}

function vpushdrop(err, kind)
{
	var counter;

	mod_assertplus.ok(isInstrumented(this),
	    'pushDrop() on uninstrumented stream');
	mod_assertplus.ok(err instanceof Error,
	    'pushDrop() called without error');
	mod_assertplus.string(kind);

	this.counter(counter);
	this.emit('drop', this.vs_context.withSource(this), kind, err);
}


/*
 * Instruments pipe() and unpipe() to update forward-and-back pointers.
 */
function instrumentPipelineOps(stream)
{
	var sticky = true;

	mod_assertplus.ok(isInstrumented(stream),
	    'attempted to instrument pipeline ops on uninstrumented stream');

	stream.vs_upstreams = [];
	stream.vs_downstreams = [];

	stream.on('pipe', function (source) {
		if (!source.hasOwnProperty('vs_downstreams')) {
			source.vs_downstreams = [];
			source.vs_upstreams = [];
		}

		source.vs_downstreams.push(stream);
		stream.vs_upstreams.push(source);
	});

	/*
	 * Node unpipes streams when the downstream emits 'finish' or 'close'.
	 * The result is that when the pipeline has finished processing all
	 * data, all of the streams become disconnected.  But this is one of the
	 * most useful cases to iterate the whole pipeline and dump counters and
	 * such.  As a result, the default behavior is that the
	 * upstream/downstream links are sticky: they survive unpipe().  This
	 * might be confusing for cases where a pipe is intentionally
	 * disconnected during normal operation, but those are relatively rare.
	 */
	if (sticky)
		return;

	stream.on('unpipe', function (source) {
		var i;

		for (i = 0; i < source.vs_downstreams.length; i++) {
			if (source.vs_downstreams[i] == stream)
				break;
		}

		mod_assertplus.ok(i < source.vs_downstreams.length);
		source.vs_downstreams.splice(i, 1);

		for (i = 0; i < stream.vs_upstreams.length; i++) {
			if (stream.vs_upstreams[i] == source)
				break;
		}

		mod_assertplus.ok(i < stream.vs_upstreams.length);
		stream.vs_upstreams.splice(i, 1);
	});
}

/*
 * Given a stream, walk back to the head of its pipeline.
 */
function streamHead(stream)
{
	mod_assertplus.ok(isInstrumented(stream));
	mod_assertplus.ok(Array.isArray(stream.vs_upstreams),
	    'stream has not been instrumented');

	while (stream.hasOwnProperty('vs_upstreams') &&
	    stream.vs_upstreams.length > 0)
		stream = stream.vs_upstreams[0];

	return (stream);
}

/*
 * Invoke "func" for each downstream stream, including the head.
 */
function streamIter(stream, func, depth)
{
	mod_assertplus.ok(Array.isArray(stream.vs_downstreams),
	    'stream has not been instrumented');

	if (!depth)
		depth = 0;

	func(stream, depth);
	stream = stream.hasOwnProperty('vs_downstreams') &&
	    stream.vs_downstreams.length > 0 ?
	    stream.vs_downstreams[0] : null;
	while (stream !== null) {
		func(stream, depth);

		/*
		 * XXX pipeline stream needs to move into this package to avoid
		 * gross interface violations.
		 * XXX doesn't handle nested pipelines
		 */
		if (stream !== null && stream.ps_streams !== undefined) {
			stream.ps_streams.forEach(
			    function (s) { func(s, depth + 1); });
		}

		stream = stream.hasOwnProperty('vs_downstreams') &&
		    stream.vs_downstreams.length > 0 ?
		    stream.vs_downstreams[0] : null;
	}
}

/*
 * Dumps debug information about a stream.
 */
function streamDump(outstream, stream, indentlen, options)
{
	var i, instr, kind, name, comments, counters, fmt;
	var indent = '';

	if (indentlen) {
		for (i = 0; i < indentlen; i++)
			indent += '    ';
	}

	if (!options)
		options = {};

	comments = [];

	if (options.showKind) {
		if (stream._readableState) {
			if (stream._writableState)
				kind = 'duplex';
			else
				kind = 'readable';
		} else if (stream._writableState) {
			kind = 'writable';
		} else {
			kind = 'unknown';
		}
		comments.push(kind);
	}

	instr = isInstrumented(stream);
	if (instr) {
		name = stream.vs_name;
	} else {
		name = stream.constructor.name;
		if (options.showKind)
			comments.push('uninstrumented');
	}

	if (stream.vs_ninputs !== undefined)
		comments.push(sprintf('%d read', stream.vs_ninputs));

	if (stream.vs_noutputs !== undefined)
		comments.push(sprintf('%d written', stream.vs_noutputs));

	if (options.showBufferInfo) {
		if (stream._writableState) {
			comments.push(sprintf('wbuf: %s/%s',
			    stream._writableState.length,
			    stream._writableState.highWaterMark));
		}

		if (stream._readableState) {
			comments.push(sprintf('rbuf: %s/%s',
			    stream._readableState.length,
			    stream._readableState.highWaterMark));
		}
	}

	outstream.write(indent);
	fmt = 20 - indent.length < 10 ? '%s' :
	    '%-' + (20 - indent.length) + 's';
	outstream.write(sprintf(fmt, name));
	if (comments.length > 0)
		outstream.write(sprintf(' (%s)', comments.join(', ')));
	outstream.write('\n');

	if (stream.hasOwnProperty('vs_counters')) {
		counters = Object.keys(stream.vs_counters).sort();
		counters.forEach(function (c) {
			outstream.write(sprintf('%s    %-16s %d\n',
			    indent, c + ':', stream.vs_counters[c]));
		});
	}
}

/*
 * A ProvenanceValue is just a wrapper for a value that keeps track of a stack
 * of provenance information.  Instances of this class are read-only, but all
 * fields are publicly accessible.
 */
function ProvenanceValue(value)
{
	this.pv_value = value;
	this.pv_provenance = [];
}

ProvenanceValue.prototype.next = function (newvalue, source)
{
	var rv;

	mod_assertplus.ok(isInstrumented(source));
	rv = new ProvenanceValue(newvalue);
	rv.pv_provenance = this.pv_provenance.slice(0);
	rv.pv_provenance.push({
	    'pvp_source': source.vs_name,
	    'pvp_input': source.vs_ninputs
	});
	return (rv);
};

ProvenanceValue.prototype.withSource = function (source)
{
	return (this.next(this.pv_value, source));
};

ProvenanceValue.prototype.label = function ()
{
	var parts;

	parts = this.pv_provenance.map(function (p) {
		return (p.pvp_source + ' input ' + p.pvp_input);
	}).reverse();

	return (parts.join(' from ') + ': value ' + this.pv_value);
};


/*
 * Convenience function for instrumenting a stream.
 */
function wrapStream(stream, options)
{
	if (typeof (options) == 'string')
		options = { 'name': options };
	instrumentObject(stream, options);
	instrumentPipelineOps(stream);
	return (stream);
}

/*
 * Convenience function for instrumenting the head of a provenance-tracking
 * Transform pipeline.
 */
function wrapTransformHead(stream, options)
{
	wrapStream(stream, options);
	instrumentTransform(stream);
	return (stream);
}

/*
 * Convenience function for instrumenting the tail of a provenance-tracking
 * Transform pipeline.
 */
function wrapTransformTail(stream, options)
{
	wrapStream(stream, options);
	instrumentTransform(stream,
	    { 'unmarshalIn': false, 'marshalOut': false });
	return (stream);
}

/*
 * Convenience function for instrumenting an inner node in a provenance-tracking
 * Transform pipeline.
 */
function wrapTransform(stream, options)
{
	wrapStream(stream, options);
	instrumentTransform(stream, { 'unmarshalIn': false });
	return (stream);
}

/*
 * Convenience function for winding back to the head of a pipeline and then
 * dumping the entire pipeline.
 */
function streamDumpPipeline(outstream, stream)
{
	streamIter(streamHead(stream), streamDump.bind(null, outstream));
}
