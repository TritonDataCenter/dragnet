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
	    'type': 'number',
	    'type': 'number'
	},
	'vmin': {
	    'type': 'number',
	    'required': true
	},
	'datasources': {
	    'type': 'array',
	    'required': true,
	    'items': {
		'type': 'object',
		'properties': {
		    'name': {
		        'type': 'string',
		        'required': true
		    },
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
	},
	'metrics': {
	    'type': 'array',
	    'required': true,
	    'items': {
		'type': 'object',
		'properties': {
		    'name': {
		        'type': 'string',
		        'required': true
		    },
		    'datasource': {
			'type': 'string',
			'required': true
		    },
		    'filter': {
			'type': 'object',
			'required': true
		    },
		    'breakdowns': {
			'type': 'array',
			'required': true,
			'items': {
			    'type': 'object',
			    'properties': {
				'name': {
				    'type': 'string',
				    'required': true
				},
				'field': {
				    'type': 'string',
				    'required': true
				},
				'date': { 'type': 'string' },
				'aggr': { 'type': 'string' },
				'step': { 'type': 'number' }
			    }
			}
		    }
		} /* end properties of entries of "metrics.items" */
	    } /* end "metrics.items" */
	} /* end "metrics" */
    }
};

function createInitialConfig()
{
	return (loadConfig({
	    'vmaj': dnConfigMajor,
	    'vmin': dnConfigMinor,
	    'datasources': [],
	    'metrics': []
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

	input.metrics.forEach(function (metconfig) {
		var metname;
		metname = metconfig.name;
		dc.dc_metrics[metname] = {
		    'm_datasource': metconfig.datasource,
		    'm_filter': metconfig.filter,
		    'm_breakdowns': metconfig.breakdowns.map(function (b) {
			var rv = {};
			mod_jsprim.forEachKey(b, function (k, v) {
				rv['b_' + k] = v;
			});
			return (rv);
		    })
		};
	});

	return (dc);
}

function DragnetConfig()
{
	this.dc_datasources = {};
	this.dc_metrics = {};
}

DragnetConfig.prototype.clone = function ()
{
	var rv;
	rv = new DragnetConfig();
	rv.dc_datasources = mod_jsprim.deepCopy(this.dc_datasources);
	rv.dc_metrics = mod_jsprim.deepCopy(this.dc_metrics);
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

DragnetConfig.prototype.metricAdd = function (metconfig)
{
	var dc;

	if (this.dc_metrics.hasOwnProperty(metconfig.name))
		return (new VError('metric "%s" already exists',
		    metconfig.name));

	dc = this.clone();
	dc.dc_metrics[metconfig.name] = {
	    'm_datasource': metconfig.datasource,
	    'm_filter': metconfig.filter,
	    'm_breakdowns': metconfig.breakdowns.map(function (b) {
		var rv = {};
		mod_jsprim.forEachKey(b, function (k, v) {
			rv['b_' + k] = v;
		});
		return (rv);
	    })
	};
	return (dc);
};

DragnetConfig.prototype.metricRemove = function (metname)
{
	var dc;

	if (!this.dc_metrics.hasOwnProperty(metname))
		return (new VError('metric "%s" does not exist', metname));

	dc = this.clone();
	delete (dc.dc_metrics[metname]);
	return (dc);
};

DragnetConfig.prototype.metricGet = function (metname)
{
	return (this.dc_metrics[metname] || null);
};

DragnetConfig.prototype.metricList = function (func)
{
	mod_jsprim.forEachKey(this.dc_metrics, func);
};

DragnetConfig.prototype.serialize = function ()
{
	/*
	 * This uses no private interfaces.
	 */
	var rv = {
	    'vmaj': dnConfigMajor,
	    'vmin': dnConfigMinor,
	    'datasources': [],
	    'metrics': []
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

	this.metricList(function (metname, m) {
		rv.metrics.push({
		    'name': metname,
		    'datasource': m.m_datasource,
		    'filter': m.m_filter,
		    'breakdowns': m.m_breakdowns.map(function (b) {
			var brv = {};
			brv.name = b.b_name;
			brv.date = b.b_date;
			brv.field = b.b_field;
			brv.aggr = b.b_aggr;
			brv.step = b.b_step;
			return (brv);
		    })
		});
	});

	return (rv);
};
