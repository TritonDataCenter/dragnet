/*
 * lib/manta-find.js: enumerate Manta objects under a directory tree, with
 * pruning.
 */

var mod_assertplus = require('assert-plus');
var mod_path = require('path');
var mod_streamutil = require('./stream-util');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vstream = require('vstream');
var VError = require('verror');

/*
 * XXX The naming here is rather misleading.
 */
var parser = require('./format-json').SkinnerReadStream;

module.exports = MantaFinder;

/*
 * Arguments:
 *
 *     manta		manta client
 *
 *     log		bunyan-style logger
 *
 *     root		root of filesystem tree to traverse
 *
 *     filter		function to invoke on directories and objects to
 *     			determine whether to descend into them or emit them
 *
 *     streamOptions	stream options (e.g., highWaterMark)
 */
function MantaFinder(args)
{
	var options;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.root, 'args.root');
	mod_assertplus.func(args.filter, 'args.filter');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	/* configuration */
	this.mf_manta = args.manta;
	this.mf_log = args.log;
	this.mf_root = args.root;
	this.mf_filter = args.filter;
	this.mf_limit = 1024;

	/* dynamic state */
	this.mf_path = null;		/* path for pending readdir() */
	this.mf_stream = null;		/* readdir() response stream */
	this.mf_dirstream = null;	/* current directory object stream */
	this.mf_queue = [ this.mf_root ];	/* queue of paths to search */
	this.mf_blocked = false;	/* waiting for dirstream */
	this.mf_marker = null;		/* marker for next request */
	this.mf_count = null;		/* dirents read so far */

	/* for debugging only */
	this.mf_response = null;	/* readdir() response object */

	options = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 128 });
	mod_stream.Readable.call(this, options);

	mod_vstream.wrapStream(this);
}

mod_util.inherits(MantaFinder, mod_stream.Readable);

MantaFinder.prototype._read = function ()
{
	this.go();
};

MantaFinder.prototype.go = function ()
{
	var path, options, entry, keepgoing;
	var self = this;

	if (this.mf_stream === null) {
		if (this.mf_path !== null) {
			this.mf_log.debug('go: already pending');
			return;
		}

		if (this.mf_queue.length === 0) {
			this.mf_log.debug('go: EOF (no stream, empty queue)');
			this.push(null);
			return;
		}

		path = this.mf_queue.shift();
		try {
			this.mf_manta.path(path);
		} catch (ex) {
			/*
			 * This can only happen for the root path, when this
			 * might well happen before the caller's had a chance to
			 * set up an 'error' listener.  A common sequence is:
			 *
			 *     finder = new MantaFinder(...)
			 *     finder.pipe(...)
			 *     return (finder);
			 *
			 * where the caller would add an error handler to
			 * "finder" after the return, but pipe() would have
			 * triggered this error right away.
			 */
			this.mf_log.debug(ex, 'go: invalid path');
			setImmediate(function () { self.emit('error', ex); });
			return;
		}

		this.mf_path = path;
		options = { 'query': { 'limit': this.mf_limit } };
		if (this.mf_marker !== null)
			options.query.marker = this.mf_marker;
		this.mf_log.debug('go: fetching', path, options);
		this.mf_manta.get(path, options,
		    function (err, stream, response) {
			var kind;

			if (err) {
				self.emit('error',
				    new VError(err, 'fetch "%s"', path));
				return;
			}

			kind = self.contentKind(response);
			if (kind == 'object') {
				response.destroy();

				self.mf_path = null;
				self.push({
				    'type': 'object',
				    'path': path
				});
				self.go();
				return;
			}

			self.mf_log.debug('go: processing directory');
			self.mf_response = response;
			self.mf_stream = stream;
			self.mf_dirstream = new parser();
			self.mf_count = 0;
			stream.pipe(self.mf_dirstream);

			self.mf_dirstream.on('end', function () {
				self.mf_log.debug('go: dir response end');
				self.mf_response = null;
				self.mf_stream = null;
				self.mf_dirstream = null;
				self.mf_path = null;

				/* XXX hack */
				if (self.mf_count == self.mf_limit) {
					self.mf_queue.unshift(path);
				} else {
					self.mf_marker = null;
				}

				self.mf_count = null;
				self.go();
			});

			self.mf_dirstream.on('readable', function () {
				if (self.mf_blocked) {
					self.mf_blocked = false;
					self.go();
				}
			});

			self.go();
		    });

		return;
	}

	this.mf_log.trace('go: reading directory');
	mod_assertplus.ok(this.mf_dirstream !== null);
	keepgoing = true;
	while (keepgoing) {
		entry = this.mf_dirstream.read(1);
		if (entry === null) {
			this.mf_log.trace('go: stream blocked');
			this.mf_blocked = true;
			return;
		}

		this.mf_log.trace('go: dirent', entry);
		this.mf_count++;
		if (entry.name == this.mf_marker)
			continue;

		this.mf_marker = entry.name;
		if (entry.type != 'directory' && entry.type != 'object') {
			this.mf_log.warn('dropping unknown entry type', entry);
			continue;
		}

		path = mod_path.join(this.mf_path, entry.name);
		if (!this.mf_filter(path)) {
			this.mf_log.trace('dropping "%s" (filtered)', path);
			continue;
		}

		if (entry.type == 'object') {
			keepgoing = this.push({
			    'type': 'object',
			    'path': path
			});
			continue;
		}

		/*
		 * XXX We should really flow-control here, or a very large
		 * directory of subdirectories could result in this queue
		 * getting very large.
		 */
		this.mf_queue.unshift(path);
	}
};

MantaFinder.prototype.contentKind = function (response)
{
	var headers, typeparts, kvpair, i;

	headers = response.headers;
	/* JSSTYLED */
	typeparts = headers['content-type'].split(/\s*;\s*/);
	if (typeparts[0].trim() != 'application/x-json-stream')
		return ('object');

	for (i = 1; i < typeparts.length; i++) {
		kvpair = typeparts[i].split('='); /* XXX need strsplit */
		if (kvpair[0].trim() == 'type')
			break;
	}

	if (i < typeparts.length && kvpair[1] == 'directory')
		return ('directory');

	return ('object');
};
