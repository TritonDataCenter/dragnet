/*
 * lib/dragnet-impl.js: common functions for internal dragnet components
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var mod_stream = require('stream');
var VError = require('verror');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_format_json = require('./format-json');

exports.asyncError = asyncError;
exports.queryAggrStream = queryAggrStream;
exports.queryAggrStreamConfig = queryAggrStreamConfig;
exports.queryTimeBoundsFilter = queryTimeBoundsFilter;
exports.parserFor = parserFor;
exports.indexQuery = indexQuery;
exports.indexConfig = indexConfig;
exports.metricSerialize = metricSerialize;
exports.metricDeserialize = metricDeserialize;
exports.metricQuery = metricQuery;

/*
 * Given an error, return a stream that will emit that error.  This is used in
 * situations where we want to emit an error from an interface that emits errors
 * asynchronously.
 */
function asyncError(err)
{
	var rv = new mod_stream.PassThrough();
	setImmediate(rv.emit.bind(rv, 'error', err));
	return (rv);
}

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
 *    query			a QueryConfig object
 *
 *    options			node-skinner options
 *
 *    [extra_bucketizers]	array of extra bucketizers to add (not part of
 *    				the query)
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

	if (args.extra_bucketizers) {
		args.extra_bucketizers.forEach(function (b) {
			options['decomps'].push(b.name);
			options['bucketizers'][b.name] = b.bucketizer;
		});
	}

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
	if (format == 'json-skinner')
		return (new mod_format_json.SkinnerReadStream());

	if (format == 'json')
		return (new mod_format_json.JsonLineStream());

	return (new VError('unsupported format: "%s"', format));
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
		    'date': '',
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

/*
 * Given properties of a new-style index, return a configuration object that we
 * could use to create the index.  Arguments:
 *
 *     datasource	Arbitrary object to be serialized directly
 *
 *     metrics		Non-empty array of metrics served by the index
 *
 *     user		Username who created the index (for auditing)
 *
 *     mtime		Timestamp when the index was updated (for auditing)
 */
function indexConfig(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.datasource, 'args.datasource');
	mod_assertplus.arrayOfObject(args.metrics, 'args.metrics');
	mod_assertplus.string(args.user, 'args.user');
	mod_assertplus.object(args.mtime, 'args.mtime');

	return ({
	    'user': args.user,
	    'mtime': args.mtime.toISOString(),
	    'datasource': args.datasource,
	    'metrics': args.metrics.map(
	        function (m) { return (metricSerialize(m, true)); })
	});
}

/*
 * Given a metric configuration object, serialize it for use in configuration
 * files.  If "skipdatasource" is true, then we don't serialize the datasource
 * name (usually because we're in a context where it's assumed).
 */
function metricSerialize(mconfig, skipdatasource)
{
	var rv = {};

	rv.name = mconfig.m_name;
	if (!skipdatasource)
		rv.datasource = mconfig.m_datasource;
	rv.filter = mconfig.m_filter;
	rv.breakdowns = mconfig.m_breakdowns.map(function (b) {
		var brv = {};
		brv.name = b.b_name;
		brv.field = b.b_field;
		if (b.b_date !== undefined)
			brv.date = b.b_date;
		if (b.b_aggr !== undefined)
			brv.aggr = b.b_aggr;
		if (b.b_step !== undefined)
			brv.step = b.b_step;
		return (brv);
	});

	return (rv);
}

/*
 * Given a metric serialization, deserialize it into the internal configuration
 * representation.
 */
function metricDeserialize(metconfig)
{
	return ({
	    'm_name': metconfig.name,
	    'm_datasource': metconfig.datasource,
	    'm_filter': metconfig.filter,
	    'm_breakdowns': metconfig.breakdowns.map(function (b) {
		var rv = {};
		mod_jsprim.forEachKey(b, function (k, v) {
			rv['b_' + k] = v;
		});
		return (rv);
	    })
	});
}

/*
 * Given a metric configuration object, return a query describing the metric.
 */
function metricQuery(metric, after, before)
{
	var queryconfig, query;
	queryconfig = metricSerialize(metric);
	queryconfig.timeAfter = after || undefined;
	queryconfig.timeBefore = before || undefined;
	query = mod_dragnet.queryLoad({ 'query': queryconfig });
	mod_assertplus.ok(!(query instanceof Error));
	return (query);
}
