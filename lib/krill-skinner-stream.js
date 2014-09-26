/*
 * XXX This functionality should probably be moved into node-krill.
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vstream = require('vstream');

module.exports = KrillSkinnerStream;

function KrillSkinnerStream(predicate, name)
{
	mod_assertplus.object(predicate, 'predicate');
	mod_assertplus.string(name, 'name');

	mod_stream.Transform.call(this, {
	    'objectMode': true,
	    'highWaterMark': 0
	});
	mod_vstream.wrapTransform(this, name);

	this.ks_predicate = predicate;
}

mod_util.inherits(KrillSkinnerStream, mod_stream.Transform);

KrillSkinnerStream.prototype._transform = function (record, _, callback)
{
	var result, error;

	mod_assertplus.object(record);
	mod_assertplus.object(record.fields, 'record.fields');
	mod_assertplus.number(record.value, 'record.value');

	try {
		result = this.ks_predicate.eval(record.fields);
	} catch (ex) {
		error = ex;
	}

	if (error) {
		this.vsWarn(error, 'nfailedeval');
	} else if (result) {
		this.push(record);
	} else {
		this.vsCounterBump('nfilteredout');
	}

	setImmediate(callback);
};
