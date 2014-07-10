/*
 * lib/fs-find.js: find files under a directory tree
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_vstream = require('./vstream/vstream');
var PipelineStream = require('./stream-pipe');

/* Public interface */
module.exports = FindStream;

/*
 * A FindStream is an object-mode stream where the caller writes strings to the
 * stream that denote filesystem paths (roots of directory trees) and the stream
 * emits objects describing the files found under the directory tree.  There are
 * no guarantees about the order of files emitted.
 *
 * Arguments:
 *
 *     log		bunyan-style logger
 *
 *     [streamOptions]	optional stream options
 *
 *
 * Implementation Notes
 *
 * This stream is a pipeline of several streams:
 *
 * - a PassThrough that exists to avoid propagating "end" directly from the
 *   upstream to the next stream.  Since this pipeline is recursive (some
 *   streams push objects back into earlier parts of the pipeline), getting an
 *   "end" from upstream is necessary but not sufficient to "end" the next
 *   stream.
 *
 * - a FindStatter: an object-mode Transform stream that takes paths as inputs
 *   and produces (path, stat object) tuples
 *
 * - a FindTraverser: an object-mode Transform stream that takes the output of a
 *   FindStatter, passes files through directly, and reads dirents in a
 *   directory.
 *
 * - an anonymous object-mode Transform stream that passes through regular
 *   files, feeds directories' entries back into the FindStatter, and ignores
 *   everything else.
 *
 * Since the Transform stream handles flow control automatically, and the
 * PipelineStream handles flow control as long as the component streams do, we
 * don't need to do anything special to manage flow control for the mainline
 * path.  But there's also a feedback path, which is not flow-controlled in any
 * way.  Even if Node provided a streaming interface for reading directories,
 * using it here could result in deadlock, with the last stream waiting on the
 * FindStatter and vice versa.  Since we ignore flow control instead, the result
 * is increased memory usage, which should only be observable for very large
 * directories.
 *
 * This implementation is pretty dubious.  It would probably be cleaner to
 * replace this with a stream that uses custom logic for managing concurrency
 * and flow control rather than trying to use a a straight pipeline with a
 * recursive path.  But without a streaming readdir() interface from Node, that
 * approach would not relieve any of the operational constraints of this
 * implementation.
 */
function FindStream(args)
{
	var passthru, statter, traverser, feedback;
	var streams, streamoptions;
	var self = this;
	var fblog;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	passthru = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	});
	mod_vstream.wrapTransform(passthru, { 'name': 'FindStart' });

	statter = new FindStatter({
	    'log': args.log.child({ 'component': 'statter' })
	});
	mod_vstream.wrapTransform(statter);

	traverser = new FindTraverser({
	    'log': args.log.child({ 'component': 'traverser' })
	});
	mod_vstream.wrapTransform(traverser);

	feedback = mod_streamutil.transformStream({
	    'streamOptions': {
	        'objectMode': true,
		'highWaterMark': 0
	    },
	    'func': function (obj, _, callback) {
		/*
		 * See "EOF Handling" below.
		 */
		if (isEofSignal(obj)) {
			if (obj.gen === self.fs_signal_generation) {
				fblog.trace('read EOF signal: ending statter');
				statter.end();
			} else {
				fblog.trace('read EOF signal, ' +
				    'but old generation');
			}

			setImmediate(callback);
			return;
		}

		if (obj.stat.isFile()) {
			fblog.trace('emit "%s": regular file', obj.path);
			this.vsCounterBump('nregfiles');
			this.push({
			    'path': obj.path,
			    'stat': obj.stat
			});
			setImmediate(callback);
			return;
		}

		if (!obj.stat.isDirectory()) {
			this.vsWarn(new Error('not file or directory'),
			    'ignored');
			setImmediate(callback);
			return;
		}

		/*
		 * For directories, we invoke the callback immediately.  If we
		 * didn't, and instead waited for the dirents to be traversed
		 * too, we'd plug up the stream.
		 *
		 * XXX The problem with this is that there's no way to both
		 * implement the intended Stream callback semantics (i.e., that
		 * the callback you provide to the FindStream is invoked when
		 * all files from that path have been emitted) *and* not plug up
		 * the stream, because the recursive entries will always be
		 * behind the ones already being processed.
		 */
		mod_assertplus.arrayOfString(obj.dirents);
		fblog.trace('processing dirents for "%s"', obj.path);
		this.vsCounterBump('ndirectories');
		obj.dirents.forEach(function (d) {
			var fullpath = mod_path.join(obj.path, d);
			fblog.trace('passing "%s" back through statter',
			    fullpath);
			statter.write(mod_path.join(obj.path, d));
		});

		/*
		 * See "EOF Handling" below.
		 */
		if (self.fs_signal_sent && obj.dirents.length > 0) {
			self.fs_signal_generation++;
			statter.write({
			    'eof': true,
			    'gen': self.fs_signal_generation
			});
		}

		setImmediate(callback);
	    }
	});
	mod_vstream.wrapTransform(feedback, { 'name': 'FindFeedback' });

	/*
	 * EOF Handling: as mentioned above, we use a pass-through to disconnect
	 * the "end" event from the caller.  Because this stream is recursive,
	 * EOF from the caller is only one of two conditions necessary to end
	 * the FindStatter.  We must also wait for the pipeline to empty out.
	 * We determine this by sending EOF signals through the pipeline, and we
	 * end the FindStatter when an EOF signal makes it all the way through
	 * without a recursive write to the FindStatter.  This feels a little
	 * unnecessarily complicated; but the alternative of instrumenting the
	 * whole pipeline to determine what's in-flight (including buffers)
	 * isn't particularly clean either.
	 */
	passthru.pipe(statter, { 'end': false });
	passthru.on('end', function () {
		self.fs_signal_sent = true;
		statter.write({
		    'eof': true,
		    'gen': self.fs_signal_generation
		});
	});
	statter.pipe(traverser);
	traverser.pipe(feedback);

	streams = [ passthru, statter, traverser, feedback ];
	streamoptions = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 512 });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions,
	    'noPipe': true
	});

	this.fs_signal_generation = -1;
	this.fs_signal_sent = false;
	this.fs_log = args.log;
	fblog = this.fs_log.child({ 'component': 'feedback' });
}

mod_util.inherits(FindStream, PipelineStream);

function isEofSignal(obj)
{
	return (obj.eof === true);
}

/*
 * Object-mode stream for statting paths.
 *
 *    input:  strings (each denoting a path)
 *
 *    output: objects with "path" and "stat" fields.
 *
 * Arguments:
 *
 *     log		bunyan-style logger
 *
 *     [streamOptions]	optional stream options
 */
function FindStatter(args)
{
	var streamoptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.st_log = args.log;

	streamoptions = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 16 });
	mod_stream.Transform.call(this, streamoptions);
}

mod_util.inherits(FindStatter, mod_stream.Transform);

FindStatter.prototype._transform = function (chunk, _, callback)
{
	var self = this;

	if (isEofSignal(chunk)) {
		this.st_log.trace('emitting EOF signal');
		this.push(chunk);
		setImmediate(callback);
		return;
	}

	this.st_log.trace('stat "%s"', chunk);
	mod_assertplus.string(chunk);
	mod_fs.stat(chunk, function (err, st) {
		if (err) {
			self.vsWarn(err, 'badstat');
			callback();
			return;
		}

		var result = {
		    'path': chunk,
		    'stat': st
		};
		self.st_log.trace(result, 'stat "%s" result', chunk);
		self.push(result);
		callback();
	});
};

/*
 * Object-mode stream for reading directories.
 *
 *    input:  objects with "path" and "stat" fields
 *            (as emitted by the FindStatter)
 *
 *    output: only objects denoting regular files and directories, the latter
 *            with a "dirent" field.
 *
 * Arguments:
 *
 *     log		bunyan-style logger
 *
 *     [streamOptions]	optional stream options
 */
function FindTraverser(args)
{
	var streamoptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.tr_log = args.log;

	streamoptions = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 16 });
	mod_stream.Transform.call(this, streamoptions);
}

mod_util.inherits(FindTraverser, mod_stream.Transform);

FindTraverser.prototype._transform = function (obj, _, callback)
{
	var self = this;

	if (isEofSignal(obj) || !obj.stat.isDirectory()) {
		if (isEofSignal(obj))
			this.tr_log.trace('emitting EOF signal');
		else
			this.tr_log.trace('emit "%s": not directory',
			    obj.path);
		this.push(obj);
		setImmediate(callback);
		return;
	}

	this.tr_log.trace('readdir "%s"', obj.path);
	mod_fs.readdir(obj.path, function (err, entries) {
		self.tr_log.trace({
		    'err': err,
		    'entries': entries
		}, 'readdir "%s" result', obj.path);
		if (err) {
			self.vsWarn(err, 'badreaddir');
			callback();
			return;
		}

		obj.dirents = entries;
		self.push(obj);
		callback();
	});
};

/*
 * XXX This mainline is used only for testing.  This should be replaced with
 * real test cases.
 */
if (require.main == module) {
	var x = new FindStream({
	    'log': new (require('bunyan'))({
	        'name': 'test',
	        'level': process.env['LOG_LEVEL'] || 'info'
	    })
	});
	process.argv.slice(2).forEach(function (path) {
		x.fs_log.trace('writing "%s"', path);
		x.write(path);
	});
	x.end();

	x.on('data', function (result) { console.log(result.path); });
	x.on('end', function () { console.log('(done)'); });
}
