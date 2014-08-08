/*
 * lib/config-common.js: internal representation of Dragnet configuration and
 * routines for updating it.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var VError = require('verror');

/* Public interface */
exports.loadConfig = loadConfig;
exports.createInitialConfig = createInitialConfig;

var dnConfigMajor = 0;
var dnConfigMinor = 0;

var dnConfigSchemaBase = {
    'type': 'object',
    'properties': {
	'vmaj': { 'type': 'number' },
	'vmin': { 'type': 'number' }
    }
};

var dnConfigSchemaCurrent = {
    'type': 'object',
    'properties': {
	'vmaj': {
	    'type': 'number'
	},
	'vmin': {
	    'type': 'number'
	},
	'datasources': {
	    'type': 'array',
	    'elements': {
		'type': 'object',
		'properties': {
		    'backend': {
		        'type': 'string',
		        'required': true
		    },
		    'backend_config': {
		        'type': 'object',
		        'required': true
		    },
		    'filter': {
		        'type': 'object',
		        'required': true
		    },
		    'dataFormat': {
		        'type': 'string'
		    }
		}
	    }
	}
    }
};

function createInitialConfig()
{
	return (loadConfig({
	    'vmaj': dnConfigMajor,
	    'vmin': dnConfigMinor,
	    'datasources': []
	}));
}

function loadConfig(input)
{
	var dc, error;

	mod_assertplus.object(input);
	error = mod_jsprim.validateJsonObject(dnConfigSchemaBase, input);
	if (error instanceof Error)
		return (new VError(error, 'failed to load config'));

	mod_assertplus.number(input.vmaj);
	mod_assertplus.number(input.vmin);
	if (input.vmaj !== dnConfigMajor)
		return (new VError('failed to load config: major ' +
		    'version ("%s") not supported', input.vmaj));

	error = mod_jsprim.validateJsonObject(dnConfigSchemaCurrent, input);
	if (error instanceof Error)
		return (new VError(error, 'failed to load config'));
	dc = new DragnetConfig();
	input.datasources.forEach(function (dsconfig) {
		dc.dc_datasources[dsconfig.name] = {
		    'ds_backend': dsconfig.backend,
		    'ds_backend_config': dsconfig.backend_config,
		    'ds_filter': dsconfig.filter,
		    'ds_format': dsconfig.dataFormat
		};
	});

	return (dc);
}

function DragnetConfig()
{
	this.dc_datasources = {};
}

DragnetConfig.prototype.clone = function ()
{
	var rv;
	rv = new DragnetConfig();
	rv.dc_datasources = mod_jsprim.deepCopy(this.dc_datasources);
	return (rv);
};

DragnetConfig.prototype.datasourceAdd = function (dsconfig)
{
	var dc;

	if (this.dc_datasources.hasOwnProperty(dsconfig.name))
		return (new VError('datasource "%s" already exists',
		    dsconfig.name));

	dc = this.clone();
	dc.dc_datasources[dsconfig.name] = {
	    'ds_backend': dsconfig.backend,
	    'ds_backend_config': dsconfig.backend_config,
	    'ds_filter': dsconfig.filter,
	    'ds_format': dsconfig.dataFormat
	};
	return (dc);
};

DragnetConfig.prototype.datasourceRemove = function (dsname)
{
	var dc;

	if (!this.dc_datasources.hasOwnProperty(dsname))
		return (new VError('datasource "%s" does not exist', dsname));

	dc = this.clone();
	delete (dc.dc_datasources[dsname]);
	return (dc);
};

DragnetConfig.prototype.datasourceGet = function (dsname)
{
	return (this.dc_datasources[dsname] || null);
};

DragnetConfig.prototype.datasourceList = function (func)
{
	mod_jsprim.forEachKey(this.dc_datasources, func);
};

DragnetConfig.prototype.serialize = function ()
{
	/*
	 * This uses no private interfaces.
	 */
	var rv = {
	    'vmaj': dnConfigMajor,
	    'vmin': dnConfigMinor,
	    'datasources': []
	};

	this.datasourceList(function (dsname, ds) {
		rv.datasources.push({
		    'name': dsname,
		    'backend': ds.ds_backend,
		    'backend_config': ds.ds_backend_config,
		    'filter': ds.ds_filter,
		    'dataFormat': ds.ds_format
		});
	});

	return (rv);
};
