/*
 * lib/datastore-local.js: implements the filesystem-based datastore, used
 * primarily for testing.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_streamutil = require('../lib/stream-util');
var mod_find = require('../lib/fs-find');
var CatStreams = require('catstreams');
var VError = require('verror');

module.exports = LocalDataStore;

function LocalDataStore(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.fsroot, 'args.fsroot');

	this.lds_log = args.log;
	this.lds_root = args.fsroot;
}

LocalDataStore.prototype.list = function (stream, errcallback)
{
	var rv = mod_find({ 'path': this.lds_root });
	rv.on('entry', function (path, st) {
		if (!st.isFile())
			return;

		stream.write({ 'name': path });
	});
	rv.on('end', function () { stream.end(); });
	rv.on('error', errcallback);
};

LocalDataStore.prototype.open = function (name, callback)
{
	setImmediate(callback, null, mod_fs.createReadStream(name));
};

LocalDataStore.prototype.stream = function ()
{
	var catstreams, xform;

	catstreams = new CatStreams({
	    'log': this.lds_log,
	    'perRequestBuffer': 16384,
	    'maxConcurrency': 1
	});

	xform = mod_streamutil.transformStream({
	    'streamOptions': { 'objectMode': true },
	    'func': function (chunk, _, callback) {
		catstreams.cat(function () {
			return (mod_fs.createReadStream(chunk.name));
		});
		callback();
	    }
	});

	xform.on('finish', function () { catstreams.cat(null); });
	this.list(xform, function (err) {
		/* XXX */
		throw (new VError(err, 'unexpected "find" error'));
	});
	return (catstreams);
};
