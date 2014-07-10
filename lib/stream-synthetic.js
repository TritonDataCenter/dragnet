/*
 * lib/stream-synthetic.js: transform stream that fills in synthetic fields for
 * a data stream.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vstream = require('./vstream/vstream');
var VError = require('verror');

/* Public interface */
module.exports = SyntheticTransformer;

/*
 * Transform stream that, given a query a configuration, populates synthetic
 * fields based on the values of other fields.
 */
function SyntheticTransformer(query)
{
	this.st_query = query;
	this.st_ninputs = 0;
	this.st_nerr_undef = 0;
	this.st_nerr_baddate = 0;

	mod_stream.Transform.call(this, {
	    'objectMode': true,
	    'highWaterMark': 0
	});

	mod_vstream.wrapTransform(this, { 'name': 'Datetime parser' });
}

mod_util.inherits(SyntheticTransformer, mod_stream.Transform);

SyntheticTransformer.prototype._transform = function (chunk, _, callback)
{
	var self = this;
	var nerrors = 0;

	this.st_ninputs++;
	this.st_query.qc_synthetic.forEach(function (fieldconf) {
		var val, parsed;

		mod_assertplus.ok(fieldconf.hasOwnProperty('date'));
		val = mod_jsprim.pluck(chunk.fields, fieldconf.field);
		if (val === undefined) {
			if (nerrors === 0) {
				self.vsWarn(new VError(
				    'field "%s" is undefined', fieldconf.field),
				    'undef');
			}
			nerrors++;
			return;
		}

		if (typeof (val) == 'number') {
			/*
			 * For convenience, we allow people to specify "date"
			 * fields that are already parsed dates.  We just pass
			 * these through.
			 */
			chunk.fields[fieldconf.name] = val;
			return;
		}

		parsed = Date.parse(val);
		if (isNaN(parsed)) {
			if (nerrors === 0) {
				self.vsWarn(
				    new VError('field "%s" is not a valid date',
				        fieldconf.field), 'baddate');
			}
			nerrors++;
			return;
		}

		chunk.fields[fieldconf.name] = Math.floor(parsed / 1000);
	});

	if (nerrors === 0)
		self.push(chunk);
	setImmediate(callback);
};
