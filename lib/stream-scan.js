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
var mod_vstream = require('./vstream/vstream');
var PipelineStream = require('./stream-pipe');
var SyntheticTransformer = require('./stream-synthetic');

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
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream = mod_vstream.wrapTransform(stream, 'user filter');
		this.ss_filter_query = stream;
		streams.push(stream);
	}

	if (this.ss_query.qc_synthetic.length > 0) {
		stream = new SyntheticTransformer(this.ss_query);
		stream = mod_vstream.wrapTransform(stream, 'datetime parser');
		streams.push(stream);
	}

	filter = mod_dragnet_impl.queryTimeBoundsFilter(args.query);
	if (filter !== null) {
		predicate = mod_krill.createPredicate(filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream = mod_vstream.wrapTransform(stream, 'time filter');
		this.ss_filter_prune = stream;
		streams.push(stream);
	}

	stream = mod_streamutil.skinnerStream();
	stream = mod_vstream.wrapTransform(stream, 'skinner adapter');
	this.ss_skinnerstream = stream;
	streams.push(this.ss_skinnerstream);

	stream = mod_dragnet_impl.queryAggrStream({
	    'query': this.ss_query,
	    'options': { 'resultsAsPoints': true }
	});
	stream = mod_vstream.wrapTransform(stream, 'aggregator');
	this.ss_aggregator = stream;
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
