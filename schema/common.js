/*
 * schema/common.js: common values and functions for JSON schemas
 */

var mod_assertplus = require('assert-plus');
var VError = require('verror');

/* Public interface */
exports.type = schemaType;
exports.enum = schemaEnum;

function schemaType(name, options)
{
	var rv = { 'type': name };
	schemaApplyOptions(rv, options);
	return (rv);
}

function schemaEnum(values, options)
{
	var rv;

	mod_assertplus.ok(values.length > 0,
	    'enum must have at least one value');
	rv = { 'type': typeof (values[0]) };
	schemaApplyOptions(rv, options);
	rv['enum'] = values.slice(0);
}

function schemaApplyOptions(rv, options)
{
	options.forEach(function (o) {
		if (o == 'required') {
			rv['required'] = true;
			return;
		}

		throw (new Error('unsupported schema option: "%s"', o));
	});
}
