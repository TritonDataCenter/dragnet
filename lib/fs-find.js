/*
 * lib/fs-find.js: find files under a directory tree
 *
 * Note that this really should be replaced with a Readable stream that supports
 * flow control, but this is sufficient for our purposes.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_util = require('util');
var mod_vasync = require('vasync');

/* Public interface */
module.exports = find;

/*
 * path		filesystem path to search
 * (string)
 *
 * Emits "result" for each found entry, with the filename (relative to "path")
 * and the result of stat(2) on the entry.  This really is only for very limited
 * use: it has no way to limit concurrency; it does not provide flexible options
 * like nftw(3C); it makes no promises about the ordering of events; and it has
 * no way to stop it (even if an error is encountered).
 */
function find(args)
{
	return (new Finder(args));
}

function Finder(args)
{
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.path, 'args.path');

	mod_events.EventEmitter.call(this);
	this.f_path = args.path;
	this.f_barrier = mod_vasync.barrier();
	this.f_barrier.on('drain', function () { self.emit('end'); });

	this.work(this.f_path);
}

mod_util.inherits(Finder, mod_events.EventEmitter);

Finder.prototype.work = function (path)
{
	var self = this;

	this.f_barrier.start(path);
	mod_fs.stat(path, function (err, st) {
		if (err) {
			self.emit('error', err);
			self.f_barrier.done(path);
			return;
		}

		self.emit('entry', path, st);
		if (!st.isDirectory()) {
			self.f_barrier.done(path);
			return;
		}

		mod_fs.readdir(path, function (err2, entries) {
			if (err2) {
				self.emit('error', err2);
				self.f_barrier.done(path);
				return;
			}

			entries.forEach(function (e) {
				self.work(mod_path.join(path, e));
			});

			self.f_barrier.done(path);
		});
	});
};

if (require.main == module) {
	var x = new Finder({ 'path': process.argv[2] });
	x.on('entry', function (name) { console.log(name); });
	x.on('end', function () { console.log('done'); });
}
