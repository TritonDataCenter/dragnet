/*
 * schema/user-query.js: JSON schema for user-faving "query" arguments
 */

var schema_common = require('./common');

module.exports = {
    'type': 'object',
    'properties': {
	'index': schema_common.type('string', [ 'required' ]),
	'timeStart': schema_common.type('string'),
	'timeEnd': schema_common.type('string'),
	'timeResolution': schema_common.type('number'),
	'filter': { 'type': 'object' },
	'breakdowns': {
	    'type': 'array',
	    'items': schema_common.type('string')
	}
    }
};
