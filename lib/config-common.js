/*
 * lib/config-common.js: internal representation of Dragnet configuration and
 * routines for updating it.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var VError = require('verror');

var mod_dragnet_impl = require('./dragnet-impl');

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
		var dsname, metname;
		dsname = metconfig.datasource;
		metname = metconfig.name;
		if (!dc.dc_metrics.hasOwnProperty(dsname))
			dc.dc_metrics[dsname] = {};
		dc.dc_metrics[dsname][metname] =
		    mod_dragnet_impl.metricDeserialize(metconfig);
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

DragnetConfig.prototype.datasourceUpdate = function (dsname, update)
{
	var dc, config;

	if (!this.dc_datasources.hasOwnProperty(dsname))
		return (new VError('datasource "%s" does not exist', dsname));

	dc = this.clone();
	config = dc.dc_datasources[dsname];

	if (update.backend)
		config.ds_backend = update.backend;

	if (update.filter)
		config.ds_filter = update.filter;

	if (update.dataFormat)
		config.ds_format = update.dataFormat;

	if (update.backend_config) {
		update = update.backend_config;
		config = config.ds_backend_config;

		if (update.path)
			config.path = update.path;
		if (update.indexPath)
			config.indexPath = update.indexPath;
		if (update.timeFormat)
			config.timeFormat = update.timeFormat;
		if (update.timeField)
			config.timeField = update.timeField;
	}

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
	var dsname, dc;

	dsname = metconfig.datasource;
	if (this.dc_metrics.hasOwnProperty(dsname) &&
	    this.dc_metrics[dsname].hasOwnProperty(metconfig.name))
		return (new VError('metric "%s" already exists',
		    metconfig.name));

	dc = this.clone();
	if (!dc.dc_metrics.hasOwnProperty(dsname))
		dc.dc_metrics[dsname] = {};
	dc.dc_metrics[dsname][metconfig.name] =
	    mod_dragnet_impl.metricDeserialize(metconfig);
	return (dc);
};

DragnetConfig.prototype.metricRemove = function (dsname, metname)
{
	var dc;

	if (!this.dc_metrics.hasOwnProperty(dsname) ||
	    !this.dc_metrics[dsname].hasOwnProperty(metname))
		return (new VError('datasource "%s" metric "%s" does not exist',
		    dsname, metname));

	dc = this.clone();
	delete (dc.dc_metrics[dsname][metname]);
	return (dc);
};

DragnetConfig.prototype.metricGet = function (dsname, metname)
{
	if (!this.dc_metrics.hasOwnProperty(dsname) ||
	    !this.dc_metrics[dsname].hasOwnProperty(metname))
		return (null);

	return (this.dc_metrics[dsname][metname]);
};

DragnetConfig.prototype.datasourceListMetrics = function (dsname, func)
{
	mod_assertplus.ok(this.dc_datasources.hasOwnProperty(dsname));
	if (!this.dc_metrics.hasOwnProperty(dsname))
		return;
	mod_jsprim.forEachKey(this.dc_metrics[dsname], func);
};

DragnetConfig.prototype.serialize = function ()
{
	var self = this;

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

		self.datasourceListMetrics(dsname, function (metname, m) {
			rv.metrics.push(mod_dragnet_impl.metricSerialize(m));
		});
	});

	return (rv);
};
