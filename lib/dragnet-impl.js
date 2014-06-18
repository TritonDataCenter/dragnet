/*
 * lib/dragnet-impl.js: common functions for internal dragnet components
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var VError = require('verror');

var JsonLineStream = require('../lib/format-json');

exports.queryAggrStream = queryAggrStream;
exports.queryAggrStreamConfig = queryAggrStreamConfig;
exports.parserFor = parserFor;

/*
 * Given a query, return a node-skinner stream that breaks down on the specified
 * fields.  Named arguments include:
 *
 *    query		a QueryConfig object
 *
 *    options		node-skinner options
 */
function queryAggrStream(args)
{
	return (mod_skinner.createAggregator(queryAggrStreamConfig(args)));
}

/*
 * Given a query, return a configuration for a node-skinner stream that breaks
 * down on the specified fields.  Named arguments include:
 *
 *    query		a QueryConfig object
 *
 *    options		node-skinner options
 */
function queryAggrStreamConfig(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.options, 'args.options');

	var options, k;
	options = {};
	if (args.options) {
		for (k in args.options)
			options[k] = args.options[k];
	}
	options['bucketizers'] = args.query.qc_bucketizers;
	options['decomps'] = args.query.qc_breakdowns.map(
	    function (b) { return (b.name); });
	options['ordinalBuckets'] = true;
	return (options);
}

/*
 * Return a parsing stream for the given file format.  The parser stream takes
 * bytes as input and produces plain JavaScript objects denoting records.
 */
function parserFor(format)
{
	if (format != 'json')
		return (new VError('unsupported format: "%s"', format));

	return (new JsonLineStream());
}
