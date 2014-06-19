/*
 * lib/stream-multiplex.js: a writable object-mode stream multiplexed over an
 * arbitrary number of backend streams based on some property of each incoming
 * write() request.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');

/* Public interface */
module.exports = MultiplexStream;

/*
 * MultiplexStream is an object-mode stream that directs incoming writes to one
 * of several downstream streams based on a bucketer() function.  The
 * prototypical use case is to partition time-series events into hourly buckets.
 * Your bucketing function could return a "YYYY-MM-DDTHH" date string extracted
 * from the incoming event.  If a corresponding downstream stream already exists
 * for that hour, the write will be directed to it.  If the stream doesn't
 * exist, your bucketCreate() function will be invoked to create it.  This
 * allows you to lazily create the streams you need as data arrives.
 *
 * bucketer()		Given an "input" object, synchronously return an object
 * 			describing the downstream stream.  This could look at
 * 			the timestamp (and possibly a "name" field or
 * 			equivalent) and returns an object whose "name" contains
 * 			the timestamp up to the hour mark.  The only field that
 * 			must be present in the returned value is "name", and
 * 			that's the only field we use.  The entire object will be
 * 			passed to your bucketCreate() function if the stream
 * 			doesn't already exist.  (The reason this is an object
 * 			rather than a string is so that you can keep the parsed
 * 			date with the stream identifier to avoid having to parse
 * 			it again in bucketCreate.)
 *
 * bucketCreate()	Given a bucket (as returned by your bucketer) and a
 * 			sample event, returns a new Writable stream for that
 * 			bucket.
 *
 * log			Bunyan-style logger
 *
 * [streamOptions]	Node Stream API options (e.g., "highWaterMark")
 */
function MultiplexStream(args)
{
	var streamoptions;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.func(args.bucketer, 'args.bucketer');
	mod_assertplus.func(args.bucketCreate, 'args.bucketCreate');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	streamoptions = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true });
	mod_stream.Writable.call(this, streamoptions);

	this.ms_log = args.log;
	this.ms_bucketer = args.bucketer;
	this.ms_bucketcreate = args.bucketCreate;
	this.ms_buckets = {};

	this.on('finish', function () {
		mod_jsprim.forEachKey(this.ms_buckets, function (name, stream) {
			self.ms_log.debug('ending "%s"', name);
			stream.end();
		});
	});
}

mod_util.inherits(MultiplexStream, mod_stream.Writable);

MultiplexStream.prototype._write = function (event, _, callback)
{
	var bucketdesc, bucketname, bucket;
	var log = this.ms_log;

	mod_assertplus.object(event, 'event');
	mod_assertplus.func(callback, 'callback');

	bucketdesc = this.ms_bucketer(event);
	if (bucketdesc === null) {
		log.trace('drop', event);
		setImmediate(callback);
		return;
	}

	mod_assertplus.object(bucketdesc, 'bucketer()');
	mod_assertplus.string(bucketdesc.name, 'bucketer().name');
	bucketname = bucketdesc.name;

	log.trace('write', event);

	if (!this.ms_buckets.hasOwnProperty(bucketname)) {
		log.trace('new bucket', bucketname);
		bucket = this.ms_bucketcreate(bucketname, bucketdesc);
		this.ms_buckets[bucketname] = bucket;
	} else {
		bucket = this.ms_buckets[bucketname];
	}

	/*
	 * XXX flow control, error handling, and deal with callback() never
	 * being invoked (as a result of an error)
	 */
	bucket.write(event, callback);
};

MultiplexStream.prototype.removeBucket = function (bucketname)
{
	mod_assertplus.ok(this.ms_buckets.hasOwnProperty(bucketname));
	delete (this.ms_buckets[bucketname]);
};
