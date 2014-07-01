/*
 * lib/stream-synthetic.js: transform stream that fills in synthetic fields for
 * a data stream.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_stream = require('stream');
var mod_util = require('util');

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
}

mod_util.inherits(SyntheticTransformer, mod_stream.Transform);

SyntheticTransformer.prototype._transform = function (chunk, _, callback)
{
	var self = this;
	var nerrors = 0;

	this.st_query.qc_synthetic.forEach(function (fieldconf) {
		var val, parsed;

		mod_assertplus.ok(fieldconf.hasOwnProperty('date'));

		self.st_ninputs++;
		val = mod_jsprim.pluck(chunk, fieldconf.field);
		if (val === undefined) {
			nerrors++;
			self.st_nerr_undef++;
			return;
		}

		parsed = Date.parse(val);
		if (isNaN(parsed)) {
			nerrors++;
			self.st_nerr_baddate++;
			return;
		}

		chunk[fieldconf.name] = Math.floor(parsed / 1000);
	});

	if (nerrors === 0)
		self.push(chunk);
	setImmediate(callback);
};
