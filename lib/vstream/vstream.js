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
exports.streamHead = streamHead;
exports.streamIter = streamIter;

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
}

function vtransform(opts, chunk, _, callback)
{
	var self = this;
	var augmented;

	mod_assertplus.ok(isInstrumented(this),
	    'attempted call to vtransform() on uninstrumented Transform');
	mod_assertplus.ok(this.vs_context === null);

	if (!opts.unmarshalIn) {
		mod_assertplus.ok(chunk instanceof ProvenanceValue);
		augmented = chunk;
	} else {
		/*
		 * Technically this is legal, but it's almost certainly not what
		 * the user wanted.
		 */
		mod_assertplus.ok(!(chunk instanceof ProvenanceValue),
		    'wrote marshaled object to VTransform that\'s ' +
		    'configured to accept bare objects');
		augmented = new ProvenanceValue(chunk);
	}

	this.vs_ninputs++;
	this.vs_context = augmented;
	this.vs_realtransform(augmented.pv_value, _, function () {
		mod_assertplus.ok(self.vs_context === augmented);
		self.vs_context = null;
		callback.apply(null, Array.prototype.slice.call(arguments));
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


/*
 * Instruments pipe() and unpipe() to update forward-and-back pointers.
 */
function instrumentPipelineOps(stream)
{
	mod_assertplus.ok(isInstrumented(stream),
	    'attempted to instrument pipeline ops on uninstrumented stream');

	stream.vs_upstreams = [];
	stream.vs_downstreams = [];

	stream.on('pipe', function (source) {
		if (isInstrumented(source))
			source.vs_downstreams.push(stream);
		stream.vs_upstreams.push(source);
	});

	stream.on('unpipe', function (source) {
		var i;

		if (!isInstrumented(source)) {
			for (i = 0; i < source.vs_downstreams.length; i++) {
				if (source.vs_downstreams[i] == stream)
					break;
			}

			mod_assertplus.ok(i < source.vs_downstreams.length);
			source.vs_downstreams.splice(i, 1);
		}

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
function streamIter(stream, func)
{
	mod_assertplus.ok(isInstrumented(stream));
	mod_assertplus.ok(Array.isArray(stream.vs_downstreams),
	    'stream has not been instrumented');

	var i = 0;
	do {
		func(stream, i++);
		stream = stream.hasOwnProperty('vs_downstreams') &&
		    stream.vs_downstreams.length > 0 ?
		    stream.vs_downstreams[0] : null;
	} while (stream !== null);
}

/*
 * Dumps debug information about a stream.
 */
function streamDump(outstream, stream)
{
	var instr, kind, name, comments;

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
	comments = [ kind ];

	instr = isInstrumented(stream);
	if (instr) {
		name = stream.vs_name;
	} else {
		name = stream.constructor.name + ' (uininstrumented)';
		comments.push('uninstrumented');
	}

	if (stream.vs_ninputs !== undefined)
		comments.push(sprintf('%d read', stream.vs_ninputs));

	if (stream.vs_noutputs !== undefined)
		comments.push(sprintf('%d written', stream.vs_noutputs));

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

	outstream.write(sprintf('%s (%s)\n', name, comments.join(', ')));
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
