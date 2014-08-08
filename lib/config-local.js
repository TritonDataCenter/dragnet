/*
 * lib/config-local.js: configuration backed by local files
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_vasync = require('vasync');
var VError = require('verror');

var mod_config_common = require('./config-common');

/* Public interface */
exports.createConfigBackend = createConfigBackend;

var dnConfigDefaultPath;

function init()
{
 	if (process.env['DRAGNET_CONFIG'])
		dnConfigDefaultPath = process.env['DRAGNET_CONFIG'];
	else
		dnConfigDefaultPath = mod_path.join(
		    process.env['HOME'], '.dragnetrc');
}

function createConfigBackend(args)
{
	var beargs;

	mod_assertplus.optionalObject(args, 'args');
	if (args !== undefined)
		mod_assertplus.optionalString(args.path, 'args.path');

	beargs = {
	    'path': args !== undefined && args.path ?
	        args.path : dnConfigDefaultPath
	};

	return (new ConfigBackendLocal(beargs));
}

function ConfigBackendLocal(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.path, 'args.path');
	this.cbl_path = args.path;
}

ConfigBackendLocal.prototype.load = function (callback)
{
	var self = this;
	var instream, data;

	instream = mod_fs.createReadStream(this.cbl_path);
	instream.on('error', function (err) {
		self.loadFinish(err, null, callback);
	});

	data = '';
	instream.on('data', function (chunk) {
		data += chunk.toString('utf8');
	});

	instream.on('end', function () {
		var parsed, error;

		try {
			parsed = JSON.parse(data);
		} catch (ex) {
			self.loadFinish(ex, null, callback);
			return;
		}

		error = mod_config_common.loadConfig(parsed);
		if (!(error instanceof Error)) {
			self.loadFinish(null, error, callback);
		} else {
			self.loadFinish(error, null, callback);
		}
	});
};

ConfigBackendLocal.prototype.loadFinish = function (error, config, callback)
{
	if (error !== null) {
		mod_assertplus.ok(config === null);
		config = mod_config_common.createInitialConfig();
	} else {
		mod_assertplus.ok(config !== null);
	}

	callback(error, config);
};

ConfigBackendLocal.prototype.save = function (config, callback)
{
	var serialized, tmpname, finalname;

	serialized = JSON.stringify(config);
	tmpname = this.cbl_path + '.tmp';
	finalname = this.cbl_path;

	mod_vasync.waterfall([
	    function saveWrite(stepcb) {
		mod_fs.writeFile(tmpname, serialized,
		    function (err) { stepcb(err); });
	    },

	    function saveRename(stepcb) {
		mod_fs.rename(tmpname, finalname,
		    function (err) { stepcb(err); });
	    }
	], function (err) {
		if (err)
			err = new VError(err, 'save "%s"', finalname);
		callback(err);
	});
};

init();
