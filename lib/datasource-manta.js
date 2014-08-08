/*
 * lib/datasource-manta.js: implementation of Datasource for Manta-based data
 * sources
 */

var mod_assertplus = require('assert-plus');
var mod_manta = require('manta');
var VError = require('verror');

var mod_source_manta = require('./source-manta');

/* Public interface */
exports.createDatasource = createDatasource;

function createDatasource(args)
{
	var dsconfig;

	mod_assertplus.object(args);
	mod_assertplus.object(args.dsconfig);
	mod_assertplus.object(args.log);

	dsconfig = args.dsconfig;
	mod_assertplus.equal(dsconfig.ds_backend, 'manta');
	if (typeof (dsconfig.ds_backend_config.path) != 'string')
		return (new VError('expected datasource "path" ' +
		    'to be a string'));
	return (new DatasourceManta(args));
}

function DatasourceManta(args)
{
	this.ds_format = args.dsconfig.ds_format;
	this.ds_timeformat = args.dsconfig.ds_backend_config.timeFormat || null;
	this.ds_path = args.dsconfig.ds_backend_config.path;
	this.ds_log = args.log;
	this.ds_manta = mod_manta.createBinClient({
	    'log': args.log.child({ 'component': 'manta' })
	});

	/* Bad, manta client! */
	process.removeAllListeners('uncaughtException');
}

DatasourceManta.prototype.scan = function (args)
{
	var source;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.assetroot, 'args.assetroot');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');

	/* XXX This functionality should eventually move into this class. */
	source = new mod_source_manta({
	    'log': this.ds_log,
	    'manta': this.ds_manta,
	    'assetroot': args.assetroot,
	    'dataroot': this.ds_path
	});

	return (source.scan({
	    'query': args.query,
	    'format': this.ds_format,
	    'dryRun': args.dryRun,
	    'timeFormat': this.ds_timeformat,
	    'extraReduceCount': 0,
	    'extraReducePhases': 0
	}));
};

DatasourceManta.prototype.close = function ()
{
	this.ds_manta.close();
};
