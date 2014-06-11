/*
 * lib/index-scan.js: execute a query over a stream of raw data
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var PipelineStream = require('./stream-pipe');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/* Public interface */
module.exports = IndexScanner;

/*
 * Object-mode Writable stream:
 *
 *     input (object-mode):  plain JavaScript objects representing records
 *
 * log			bunyan-style logger
 *
 * index		index configuration (see schema)
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 *
 * XXX consider generalizing "time" to "primary key" and having the previous
 * stream in the pipeline normalize records to put the value in a known format
 * in a known field?
 */
function IndexScanner(args)
{
	var streamoptions, streams;
	var predicate, stream;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.is_log = args.log;
	this.is_index = args.index;
	this.is_filter = null;
	this.is_ntransformed = 0;

	streams = [];
	if (args.index.filter) {
		/* XXX handle syntax error */
		predicate = mod_krill.createPredicate(args.index.filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		streams.push(stream);
		this.is_filter = stream;
	}

	/* XXX create predicate streams for timeStart, timeEnd */

	streams.push(mod_streamutil.transformStream({
	    'streamOptions': { 'objectMode': true },
	    'func': function (chunk, _, callback) {
		this.is_ntransformed++;
		this.push({
		    'fields': chunk,
		    'value': 1
		});
		callback();
	    }
	}));

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(IndexScanner, PipelineStream);

IndexScanner.prototype.stats = function ()
{
	var stats = {};
	var dsstats;

	if (this.is_filter !== null) {
		dsstats = this.is_filter.stats();
		stats.ninputs = dsstats.ninputs;
		stats.index_filter_nremoved = dsstats.nfilteredout;
		stats.index_filter_nerrors = dsstats.nerrors;
	} else {
		stats.index_filter_nremoved = 0;
		stats.index_filter_nerrors = 0;
	}

	if (!stats.hasOwnProperty('ninputs'))
		stats.ninputs = this.is_ntransformed;

	return (stats);
};
