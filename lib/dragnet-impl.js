/*
 * lib/dragnet-impl.js: common functions for internal dragnet components
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var VError = require('verror');

var mod_dragnet = require('./dragnet'); /* XXX */
var JsonLineStream = require('./format-json');

exports.queryAggrStream = queryAggrStream;
exports.queryAggrStreamConfig = queryAggrStreamConfig;
exports.queryTimeBoundsFilter = queryTimeBoundsFilter;
exports.parserFor = parserFor;
exports.indexQuery = indexQuery;

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
 * Return a krill filter for the the query's time bounds.
 */
function queryTimeBoundsFilter(query)
{
	var timefield, filter;

	if (query.qc_before !== null) {
		mod_assertplus.ok(query.qc_after !== null);
		timefield = query.qc_timefield.name;

		/*
		 * Since the start time is inclusive and measured in
		 * milliseconds, but we only index in seconds, we round
		 * up to the nearest second and use a "greater-than-or-
		 * equal-to" condition.  Similarly, we round up the end
		 * time and use a "less-than" condition.
		 */
		filter = {
		    'and': [ {
		        'ge': [ 'PLACEHOLDER', Math.ceil(
			    query.qc_after.getTime() / 1000) ]
		    }, {
		        'lt': [ 'PLACEHOLDER', Math.ceil(
			    query.qc_before.getTime() / 1000) ]
		    } ]
		};
		filter.and[0].ge[0] = timefield;
		filter.and[1].lt[0] = timefield;
		return (filter);
	} else {
		mod_assertplus.ok(query.qc_after === null);
		return (null);
	}
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

/*
 * Given an index configuration, return a query whose results would match the
 * desired contents of the index.  Since the index has already been validated
 * and is more constrained than a query, this should not return an error.
 *
 * If the index has a time field, then an additional column, __dn_ts, is added
 * with sufficient resolution to divide results accordingly.  For example, if
 * you're generating hourly indexes, __dn_ts will be aggregated by hour so that
 * you can use this field to divide results by hour.
 */
function indexQuery(index, derived)
{
	var step, columns, query;

	columns = index.ic_columns.slice(0);

	if (!derived && index.ic_timefield !== null) {
		if (index.ic_interval == 'hour') {
			step = 3600;
		} else {
			mod_assertplus.equal(index.ic_interval, 'day');
			step = 86400;
		}

		columns.push({
		    'name': '__dn_ts',
		    'aggr': 'lquantize',
		    'step': step,
		    'date': true,
		    'field': index.ic_timefield.name
		});
	}

	query = mod_dragnet.queryLoad({
	    'allowReserved': true,
	    'query': {
		'filter': index.ic_filter,
		'breakdowns': columns,
		'timeAfter': index.ic_after || undefined,
		'timeBefore': index.ic_before || undefined
	    }
	});

	mod_assertplus.ok(!(query instanceof Error), query.message);
	return (query);
}
