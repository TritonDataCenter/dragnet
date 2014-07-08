/*
 * vstream.js: instrumentable streams mix-ins
 */

var mod_assertplus = require('assert-plus');
var mod_streamutil = require('../stream-util');

/* Public interface */
exports.instrumentObject = instrumentObject;
exports.instrumentTransform = instrumentTransform;

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

	this.vs_ninputs++;
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

	if (chunk === null)
		augmented = null;
	else
		augmented = this.vs_context.next(chunk, this);

	return (this.vs_realpush(augmented));
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
