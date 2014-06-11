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
 * [streamOptions]	options to pass through to Node.js Stream constructor
 *
 * XXX consider generalizing "time" to "primary key" and having the previous
 * stream in the pipeline normalize records to put the value in a known format
 * in a known field?
 */
function QueryScanner(args)
{
	var streamoptions, stream, streams;
	var predicate, breakdowns, bucketizers, columndefs;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.qs_log = args.log;
	this.qs_index = args.index;
	this.qs_query = args.query;
	this.qs_bucketizer = null;

	streams = [];
	if (args.index.filter) {
		/* XXX handle syntax error */
		predicate = mod_krill.createPredicate(args.index.filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		streams.push(stream);
	}

	/* XXX create predicate streams for timeStart, timeEnd */

	if (args.query.filter) {
		predicate = mod_krill.createPredicate(args.query.filter);
		stream = mod_krill.createPredicateStream(
		    { 'predicate': predicate });
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		streams.push(stream);
	}

	streams.push(mod_streamutil.transformStream({
	    'streamOptions': { 'objectMode': true },
	    'func': function (chunk, _, callback) {
		this.push({
		    'fields': chunk,
		    'value': 1
		});
		callback();
	    }
	}));

	/* XXX compile based on arguments */
	breakdowns = [];
	bucketizers = {};
	columndefs = {};

	if (args.query.breakdowns) {
		args.index.columns.forEach(function (c) {
			if (typeof (c) == 'string') {
				columndefs[c] = {
				    'name': c,
				    'field': c
				};
			} else {
				columndefs[c.name] = c;
			}
		});
		args.query.breakdowns.forEach(function (b, i) {
			var coldef;

			mod_assertplus.string(b, 'query breakdown[' + i + ']');
			if (!columndefs.hasOwnProperty(b)) {
				/*
				 * XXX The user has asked to run a scan using a
				 * field that's not in the index.  There's
				 * currently no way for them to indicate if it's
				 * a numeric field.
				 */
				breakdowns.push(b);
				return;
			}

			coldef = columndefs[b];
			if (!coldef.hasOwnProperty('aggr')) {
				breakdowns.push(coldef.field);
				return;
			}

			mod_assertplus.equal(coldef.aggr, 'quantize');
			mod_assertplus.ok(i == args.query.breakdowns.length - 1,
			    'quantized breakdowns must be last');
			breakdowns.push(coldef.field);
			bucketizers[coldef.field] =
			    mod_skinner.makeP2Bucketizer();
			self.qs_bucketizer = bucketizers[coldef.field];
		});
	}

	streams.push(mod_skinner.createAggregator({
	    'bucketizers': bucketizers,
	    'ordinalBuckets': true,
	    'decomps': breakdowns
	}));

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
