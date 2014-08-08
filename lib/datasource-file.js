/*
 * lib/datasource-file.js: implementation of Datasource for file-based data
 * sources
 */

var mod_assertplus = require('assert-plus');
var mod_source_file = require('./source-fileset');
var VError = require('verror');

exports.createDatasource = createDatasource;

function createDatasource(args)
{
	var dsconfig;

	mod_assertplus.object(args);
	mod_assertplus.object(args.dsconfig);
	mod_assertplus.object(args.log);

	dsconfig = args.dsconfig;
	mod_assertplus.equal(dsconfig.ds_backend, 'file');
	if (typeof (dsconfig.ds_backend_config.path) != 'string')
		return (new VError('expected datasource "path" ' +
		    'to be a string'));
	return (new DatasourceFile(args));
}

function DatasourceFile(args)
{
	this.ds_format = args.dsconfig.ds_format;
	this.ds_timeformat = args.dsconfig.ds_backend_config.timeFormat || null;
	this.ds_path = args.dsconfig.ds_backend_config.path;
	this.ds_log = args.log;
}

DatasourceFile.prototype.scan = function (args)
{
	var source;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');

	/*
	 * XXX This functionality should eventually move into this class.
	 * XXX dryRun is not implemented by this class.
	 */
	source = new mod_source_file({
	    'log': this.ds_log,
	    'dataroot': this.ds_path
	});

	return (source.scan({
	    'query': args.query,
	    'format': this.ds_format,
	    'dryRun': args.dryRun,
	    'timeFormat': this.ds_timeformat
	}));
};

DatasourceFile.prototype.close = function ()
{
};
