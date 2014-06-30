/*
 * lib/stream-scan.js: a stream scanner that filters and aggregates the contents
 * of a stream.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var PipelineStream = require('./stream-pipe');

var mod_dragnet_impl = require('./dragnet-impl');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

module.exports = StreamScan;

/*
 * Object-mode "transform" stream:
 *
 *     input (object-mode):  plain JavaScript objects representing records
 *
 *     output (object-mode): plain JavaScript summary of query results
 *
 * log			bunyan-style logger
 *
 * query		QueryConfig object
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 */
function StreamScan(args)
{
	var predicate, stream, streams, filter, streamoptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.query.filter, 'args.query.filter');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.ss_log = args.log;
	this.ss_query = args.query;
	this.ss_filter_query = null;
	this.ss_skinnerstream = null;
	this.ss_aggregator = null;
	this.ss_filter_prune = null;

	streams = [];

	if (args.query.qc_filter) {
		predicate = mod_krill.createPredicate(args.query.qc_filter);
		this.ss_filter_query = stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		streams.push(stream);
	}

	if (this.ss_query.qc_synthetic.length > 0)
		streams.push(new SyntheticTransformer(this.ss_query));

	filter = mod_dragnet_impl.queryTimeBoundsFilter(args.query);
	if (filter !== null) {
		predicate = mod_krill.createPredicate(filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		this.ss_filter_prune = stream;
		streams.push(stream);
	}

	this.ss_skinnerstream = mod_streamutil.skinnerStream();
	streams.push(this.ss_skinnerstream);

	this.ss_aggregator = stream = mod_dragnet_impl.queryAggrStream({
	    'query': this.ss_query,
	    'options': { 'resultsAsPoints': true }
	});
	stream.on('invalid_object', this.emit.bind(this, 'invalid_object'));
	streams.push(stream);

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true }, { 'highWaterMark': 0 });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(StreamScan, PipelineStream);

StreamScan.prototype.stats = function ()
{
	var stats = {};
	var dsstats;

	if (this.ss_filter_query !== null) {
		dsstats = this.ss_filter_query.stats();
		stats.ninputs = dsstats.ninputs;
		stats.query_filter_nremoved = dsstats.nfilteredout;
		stats.query_filter_nerrors = dsstats.nerrors;
	} else {
		stats.query_filter_nremoved = 0;
		stats.query_filter_nerrors = 0;
	}

	if (!stats.hasOwnProperty('ninputs'))
		stats.ninputs = this.ss_skinnerstream.ntransformed;

	dsstats = this.ss_aggregator.stats();
	stats.aggr_nprocessed = dsstats.ninputs;
	stats.aggr_nerr_nonnumeric = dsstats.nerr_nonnumeric;
	return (stats);
};


/*
 * Transform stream that, given a query a configuration, populates synthetic
 * fields based on the values of other fields.
 */
function SyntheticTransformer(query)
{
	this.st_query = query;
	this.st_ninputs = 0;
	this.st_nerr_undef = 0;
	this.st_nerr_baddate = 0;
	mod_stream.Transform.call(this, {
	    'objectMode': true,
	    'highWaterMark': 0
	});
}

mod_util.inherits(SyntheticTransformer, mod_stream.Transform);

SyntheticTransformer.prototype._transform = function (chunk, _, callback)
{
	var self = this;
	var nerrors = 0;

	this.st_query.qc_synthetic.forEach(function (fieldconf) {
		var val, parsed;

		mod_assertplus.ok(fieldconf.hasOwnProperty('date'));

		self.st_ninputs++;
		val = mod_jsprim.pluck(chunk, fieldconf.field);
		if (val === undefined) {
			nerrors++;
			self.st_nerr_undef++;
			return;
		}

		parsed = Date.parse(val);
		if (isNaN(parsed)) {
			nerrors++;
			self.st_nerr_baddate++;
			return;
		}

		chunk[fieldconf.name] = Math.floor(parsed / 1000);
	});

	if (nerrors === 0)
		self.push(chunk);
	setImmediate(callback);
};
