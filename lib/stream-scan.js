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
var mod_vstream = require('vstream');
var PipelineStream = mod_vstream.PipelineStream;
var SyntheticTransformer = require('./stream-synthetic');
var KrillSkinnerStream = require('./krill-skinner-stream');

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
 * timeField		field denoting time (used if query specifies --before
 * 			and --after)
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 */
function StreamScan(args)
{
	var query, predicate, stream, streams, filter, streamoptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.query.filter, 'args.query.filter');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	query = args.query;
	streams = [];

	this.ss_log = args.log;
	this.ss_query = args.query;

	if (args.query.qc_filter) {
		predicate = mod_krill.createPredicate(args.query.qc_filter);
		stream = new KrillSkinnerStream(predicate, 'User filter');
		streams.push(stream);
	}

	if (query.qc_before !== null || query.qc_after !== null) {
		mod_assertplus.string(args.timeField, 'args.timeField');
		this.ss_query.qc_synthetic.push({
		    'name': 'dn_ts',
		    'field': args.timeField,
		    'date': ''
		});
	}

	if (this.ss_query.qc_synthetic.length > 0)
		streams.push(new SyntheticTransformer(this.ss_query));

	filter = mod_dragnet_impl.queryTimeBoundsFilter(args.query, 'dn_ts');
	if (filter !== null) {
		predicate = mod_krill.createPredicate(filter);
		stream = new KrillSkinnerStream(predicate, 'Time filter');
		streams.push(stream);
	}

	stream = mod_dragnet_impl.queryAggrStream({
	    'query': this.ss_query,
	    'options': { 'resultsAsPoints': true }
	});
	stream = mod_vstream.wrapTransform(stream, 'Aggregator');
	streams.push(stream);

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true }, { 'highWaterMark': 0 });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions
	});
}

mod_util.inherits(StreamScan, PipelineStream);
