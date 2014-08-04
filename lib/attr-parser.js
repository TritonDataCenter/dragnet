/*
 * lib/attr-parser.js: parse a list of dragnet-style fields.  Fields are
 * comma-separated, and each may include a comma-separated list of attributes
 * (with optional values) in square brackets.  Examples include:
 *
 *     field1
 *     field1,field2
 *     field1[attr1=value1,attr2=value2,attr3],field2[attr2],field3
 *
 * On error, an Error object is returned (not thrown).
 */

var mod_assert = require('assert');

module.exports = attrsParse;

function attrsParse(str)
{
	var i, j, eq, propname, props, propdef, rv;

	propname = null;
	props = null;
	rv = [];
	for (i = 0, j = 0; i < str.length; i++) {
		if (propname === null) {
			mod_assert.ok(props === null);
			if (str.charAt(i) == ',') {
				if (i - j > 0)
					rv.push(
					    { 'name': str.substr(j, i - j) });
				j = i + 1;
			} else if (str.charAt(i) == '[') {
				if (i - j === 0)
					return (new Error(
					    'missing field name'));
				propname = str.substr(j, i - j);
				props = { 'name': propname };
				j = i + 1;
			}

			continue;
		}

		mod_assert.ok(props !== null);
		if (str.charAt(i) == ',' || str.charAt(i) == ']') {
			if (i - j > 0) {
				propdef = str.substr(j, i - j);
				eq = propdef.indexOf('=');
				if (eq == -1) {
					props[propdef] = '';
				} else if (eq === 0) {
					return (new Error(
					    'missing attribute name'));
				} else {
					props[propdef.substr(0, eq)] =
					    propdef.substr(eq + 1);
				}
			}

			if (str.charAt(i) == ']') {
				rv.push(props);
				propname = null;
				props = null;
			}

			j = i + 1;
		}
	}

	if (propname !== null)
		return (new Error('unexpected end of string'));

	if (j < str.length - 1)
		rv.push({ 'name': str.substr(j) });

	return (rv);
}
