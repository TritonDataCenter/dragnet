/*
 * lib/query-scan.js: execute a query over a stream of raw data
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var PipelineStream = require('./stream-pipe');

/* XXX should this be a dragnet-impl.js instead? */
var mod_dragnet = require('./dragnet');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/* Public interface */
module.exports = QueryScanner;

/*
 * Object-mode "transform" stream:
 *
 *     input (object-mode):  plain JavaScript objects representing records
 *
 *     output (object-mode): plain JavaScript summary of query results
 *
 * log			bunyan-style logger
 *
 * index		index configuration (see schema)
 *
 * query		query configuration (see schema)
 *
 * resultsAsPoints	report results as data points (see node-skinner)
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 *
 * XXX consider generalizing "time" to "primary key" and having the previous
 * stream in the pipeline normalize records to put the value in a known format
 * in a known field?
 */
function QueryScanner(args)
{
	var streamoptions, stream, streams;
	var predicate, skinnerconf;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');
	mod_assertplus.optionalBool(args.resultsAsPoints,
	    'args.resultsAsPoints');

	this.qs_log = args.log;
	this.qs_index = args.index;
	this.qs_query = args.query;
	this.qs_bucketizer = null;
	this.qs_filter_index = null;
	this.qs_filter_query = null;
	this.qs_skinnerstream = null;

	streams = [];
	if (this.qs_index.ic_filterstream) {
		stream = this.qs_index.ic_filterstream;
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		this.qs_filter_index = stream;
		streams.push(stream);
	}

	/* XXX create predicate streams for timeStart, timeEnd */

	if (args.query.filter) {
		/* XXX have a queryLoad that handles syntax errors and such? */
		predicate = mod_krill.createPredicate(args.query.filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		streams.push(stream);
		this.qs_filter_query = stream;
	}

	this.qs_skinnerstream = mod_streamutil.skinnerStream();
	streams.push(this.qs_skinnerstream);

	skinnerconf = mod_dragnet.indexAggregator({
	    'index': this.qs_index,
	    'breakdowns': args.query.breakdowns || [],
	    'allowExternal': true,
	    'options': {
	        'resultsAsPoints': args.resultsAsPoints
	    }
	});
	if (skinnerconf instanceof Error) {
		/* XXX need better way to report this */
		throw (skinnerconf);
	}
	this.qs_bucketizer = skinnerconf.bucketizer;
	stream = skinnerconf.stream;
	stream.on('invalid_object', this.emit.bind(this, 'invalid_object'));
	streams.push(stream);
	this.qs_aggregator = stream;

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(QueryScanner, PipelineStream);

QueryScanner.prototype.bucketizer = function ()
{
	return (this.qs_bucketizer);
};

QueryScanner.prototype.stats = function ()
{
	var stats = {};
	var dsstats;

	if (this.qs_filter_index !== null) {
		dsstats = this.qs_filter_index.stats();
		stats.ninputs = dsstats.ninputs;
		stats.index_filter_nremoved = dsstats.nfilteredout;
		stats.index_filter_nerrors = dsstats.nerrors;
	} else {
		stats.index_filter_nremoved = 0;
		stats.index_filter_nerrors = 0;
	}

	if (this.qs_filter_query !== null) {
		dsstats = this.qs_filter_query.stats();
		if (this.qs_filter_index === null)
			stats.ninputs = dsstats.ninputs;
		stats.query_filter_nremoved = dsstats.nfilteredout;
		stats.query_filter_nerrors = dsstats.nerrors;
	} else {
		stats.query_filter_nremoved = 0;
		stats.query_filter_nerrors = 0;
	}

	if (!stats.hasOwnProperty('ninputs'))
		stats.ninputs = this.qs_skinnerstream.ntransformed;

	dsstats = this.qs_aggregator.stats();
	stats.aggr_nprocessed = dsstats.ninputs;
	stats.aggr_nerr_nonnumeric = dsstats.nerr_nonnumeric;
	return (stats);
};
