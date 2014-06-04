/*
 * lib/datastore-local.js: implements the filesystem-based datastore, used
 * primarily for testing.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_find = require('../lib/fs-find');

module.exports = LocalDataStore;

function LocalDataStore(args)
{
	mod_assertplus.string(args.fsroot, 'args.fsroot');
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
