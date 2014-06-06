/*
 * schema/user-index.js: JSON schema for user-facing "index" configuration
 */

var schema_common = require('./common');
var type = schema_common.type;

module.exports = {
    'type': 'object',
    'properties': {
	'name': schema_common.type('string', [ 'required' ]),
	'fsroot': schema_common.type('string'),
	'mantaroot': schema_common.type('string'),
	'format': schema_common.enum([ 'json' ], [ 'required' ]),
	'filter': { 'type': 'object' },
	'primaryKey': schema_common.type('string'),
	'columns': {
	    'type': 'array',
	    'required': true,
	    'items': {
		'type': [ 'string', {
		    'type': 'object',
		    'properties': {
			'name': schema_common.type('string', [ 'required' ]),
			'field': schema_common.type('string', [ 'required' ]),
			'aggr': schema_common.enum([ 'quantize' ])
		    }
		} ]
	    }
	}
    }
};
