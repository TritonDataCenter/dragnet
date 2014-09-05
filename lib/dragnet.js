/*
 * lib/dragnet.js: dragnet library interface
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var VError = require('verror');

var mod_datasource_file = require('./datasource-file');
var mod_datasource_manta = require('./datasource-manta');
var mod_dragnet_impl = require('./dragnet-impl');

/* Public interface */
exports.queryLoad = queryLoad;
exports.build = build;
exports.indexConfig = indexConfig;
exports.indexScan = indexScan;
exports.indexRead = indexRead;
exports.datasourceForConfig = datasourceForConfig;
exports.datasourceForName = datasourceForName;

/*
 * This is a struct-like class that represents the immutable parameters of a
 * specific query.
 */
function QueryConfig(args)
{
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.optionalObject(args.filter, 'args.filter');
	mod_assertplus.arrayOfObject(args.breakdowns, 'args.breakdowns');

	this.qc_filter = args.filter || null;
	this.qc_breakdowns = mod_jsprim.deepCopy(args.breakdowns);
	this.qc_before = args.timeBefore || null;
	this.qc_after = args.timeAfter || null;
	this.qc_fieldsbyname = {};
	this.qc_bucketizers = {};
	this.qc_synthetic = [];

	if (args.timeField) {
		this.qc_synthetic.push({
		    'name': args.timeField,
		    'field': args.timeField,
		    'date': ''
		});
	}

	this.qc_breakdowns.forEach(function (fieldconf) {
		self.qc_fieldsbyname[fieldconf.name] = fieldconf;

		if (fieldconf.hasOwnProperty('date'))
			self.qc_synthetic.push(fieldconf);

		if (!fieldconf.hasOwnProperty('aggr'))
			return;

		if (fieldconf.aggr == 'quantize') {
			self.qc_bucketizers[fieldconf.name] =
			    mod_skinner.makeP2Bucketizer();
			return;
		}

		mod_assertplus.equal(fieldconf.aggr, 'lquantize');
		mod_assertplus.number(fieldconf.step);
		self.qc_bucketizers[fieldconf.name] =
		    mod_skinner.makeLinearBucketizer(fieldconf.step);
	});

	if (this.qc_before !== null)
		mod_assertplus.ok(this.qc_after !== null);
	else
		mod_assertplus.ok(this.qc_after === null);
}

/*
 * Normalize and validate the requested query.  On success, returns a
 * QueryConfig object that describes the query parameters.  On failure, returns
 * an Error describing what's invalid.  Named arguments include:
 *
 *     query		describes the query parameters, including:
 *
 *         [filter]		node-krill-syntax plain-JS-object filter
 *
 *         breakdowns		List of fields to break out results by.  These
 *         			should be strings in the same format as for
 *         			index definitions.
 *
 *         [timeAfter]		Prune indexes covering times before this time.
 *
 *         [timeBefore]		Prune indexes covering times after this time.
 *
 *         [timeField]		Extra field (not part of the breakdowns) that's
 *         			used for processing "before" and "after"
 *         			constraints.
 *
 *     allowReserved	Allow reserved field names to be used.  This should only
 *     			be used internally.
 */
function queryLoad(args)
{
	var filter, breakdowns, timebounds;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.query.filter, 'args.query.filter');
	mod_assertplus.ok(Array.isArray(args.query.breakdowns));

	if (args.query.filter) {
		filter = args.query.filter;

		try {
			mod_krill.createPredicate(filter);
		} catch (ex) {
			return (new VError(ex,
			    'invalid query: invalid filter'));
		}
	} else {
		filter = null;
	}

	breakdowns = parseFields(args.query.breakdowns,
	    { 'allowReserved': args.allowReserved });
	if (breakdowns instanceof Error)
		return (new VError(breakdowns, 'invalid query'));

	timebounds = parseTimeBounds({
	    'timeAfter': args.query.timeAfter,
	    'timeBefore': args.query.timeBefore
	});
	if (timebounds instanceof Error)
		return (timebounds);

	return (new QueryConfig({
	    'filter': filter,
	    'breakdowns': breakdowns,
	    'timeAfter': timebounds.timeAfter,
	    'timeBefore': timebounds.timeBefore,
	    'timeField': args.query.timeField
	}));
}

/*
 * Parse and validate the "before" and "after" timestamps.  Because of the way
 * we iterate files and objects later, we can only support neither or both of
 * these fields.
 */
function parseTimeBounds(args)
{
	var timeBefore, timeAfter;

	/*
	 * Check the "before" and "after" fields.  Because of the way we iterate
	 * them, we can only support neither or both.
	 */
	if (args.timeAfter) {
		if (!args.timeBefore) {
			return (new VError(
			    '"after" requires specifying "before" too'));
		}

		timeAfter = new Date(args.timeAfter);
		if (isNaN(timeAfter.getTime()))
			return (new VError('"after": not a valid date: "%s"',
			    args.timeAfter));

		timeBefore = new Date(args.timeBefore);
		if (isNaN(timeBefore.getTime()))
			return (new VError('"before": not a valid date: "%s"',
			    args.timeBefore));

		if (timeAfter.getTime() > timeBefore.getTime())
			return (new VError('"after" timestamp may not ' +
			    'come after "before"'));
	} else if (args.timeBefore) {
		return (new VError('"before" requires specifying "after" too'));
	}

	return ({
	    'timeAfter': timeAfter,
	    'timeBefore': timeBefore
	});
}

/*
 * Parse an array of columns, as specified for both a query and an index.
 */
function parseFields(inputs, options)
{
	var fields, i, b, ret;

	fields = new Array(inputs.length);
	for (i = 0; i < inputs.length; i++) {
		b = inputs[i];
		ret = parseField(b, options);
		if (ret instanceof Error) {
			return (new VError(ret,
			    'field %d ("%s") is invalid', i, b));
		}

		fields[i] = ret;
	}

	return (fields);
}

function parseField(b, options)
{
	var step;

	mod_assertplus.ok(typeof (b) != 'string');
	mod_assertplus.string(b['name']);
	if (b.hasOwnProperty('aggr')) {
		if (b['aggr'] != 'quantize' && b['aggr'] != 'lquantize')
			return (new VError(
			    'unsupported aggr: "%s"', b['aggr']));

		if (b['aggr'] == 'lquantize') {
			if (!b.hasOwnProperty('step'))
				return (new VError('aggr "lquantize" ' +
				    'requires "step"'));

			step = parseInt(b['step'], 10);
			if (isNaN(step))
				return (new VError('aggr "lquzntize":' +
				    ' invalid value for "step": "%s"',
				    b['step']));
			b['step'] = step;
		}
	}

	if ((!options || !options.allowReserved) &&
	    mod_jsprim.startsWith(b.name, '__dn'))
		return (new VError('field names starting with ' +
		    '"__dn" are reserved'));

	if (!b.hasOwnProperty('field'))
		b['field'] = b['name'];

	return (b);
}

function hasDateField(columns)
{
	for (var i = 0; i < columns.length; i++) {
		if (columns[i].hasOwnProperty('date'))
			return (true);
	}

	return (false);
}

/*
 * Returns a Datasource implementation for the given configuration and
 * datasource name.
 */
function datasourceForName(args)
{
	var dsconfig, datasource;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');

	dsconfig = args.config.datasourceGet(args.dsname);
	if (dsconfig === null)
		return (new VError('unknown datasource: "%s"', args.dsname));

	datasource = datasourceForConfig({
	    'log': args.log,
	    'dsconfig': dsconfig
	});

	if (datasource instanceof Error)
		return (datasource);

	return (datasource);
}

/*
 * Returns a Datasource implementation for the given datasource configuration.
 * See config-common for the configuration structure.
 */
function datasourceForConfig(args)
{
	var bename;

	mod_assertplus.object(args);
	mod_assertplus.object(args.dsconfig);
	mod_assertplus.object(args.log);

	bename = args.dsconfig.ds_backend;

	if (bename == 'manta')
		return (mod_datasource_manta.createDatasource(args));
	if (bename == 'file')
		return (mod_datasource_file.createDatasource(args));

	return (new VError('unknown datasource backend: "%s"', bename));
}

/*
 * Build all indexes for the given datasource.  Arguments:
 *
 *     log		bunyan-style logger
 *
 *     config		dragnet configuration
 *
 *     indexConfig	index configuration, which overrides metrics stored in
 *     			the configuration
 *
 *     dsname		datasource name
 *
 *     dryRun		print what would be done to stderr, but don't do
 *     			anything.
 *
 *     interval		"day" or "hour", referring to how to chunk up separate
 *     			index files
 *
 *     [timeAfter]	don't scan data with timestamp before this
 *
 *     [timeBefore]	don't scan data with timestamp after this
 *
 *     [assetroot]	required for Manta
 */
function build(args, callback)
{
	var error, config, dsname, log, ds;
	var metrics;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.string(args.interval, 'args.interval');
	mod_assertplus.optionalString(args.assetroot, 'args.assetroot');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	config = args.config;
	dsname = args.dsname;
	log = args.log;
	error = null;

	if (args.timeBefore !== null && args.timeAfter !== null &&
	    args.timeBefore.getTime() < args.timeAfter.getTime())
		error = new VError(
		    '"before" time cannot be before "after" time');

	switch (args.interval) {
	case 'hour':
	case 'day':
	case 'all':
		break;
	default:
		error = new VError('interval not supported: "%s"',
		    args.interval);
		break;
	}

	if (error !== null) {
		setImmediate(callback, error);
		return (null);
	}

	ds = datasourceForName({
	    'log': log,
	    'config': config,
	    'dsname': dsname
	});
	if (ds instanceof Error) {
		setImmediate(callback, ds);
		return (null);
	}

	metrics = metricsForIndex(args);
	if (metrics.length === 0) {
		setImmediate(callback,
		    new VError('no metrics defined for dataset "%s"', dsname));
		return (null);
	}

	return (ds.build({
	    'metrics': metrics,
	    'dryRun': args.dryRun,
	    'interval': args.interval,
	    'assetroot': args.assetroot,
	    'timeAfter': args.timeAfter,
	    'timeBefore': args.timeBefore
	}, function (err) { callback(err, ds); }));
}

/*
 * Generate the index configuration for the given datasource.  Arguments:
 *
 *     config		dragnet configuration
 *
 *     dsname		datasource name
 */
function indexConfig(args, callback)
{
	var error, config, dsname, dsconfig;
	var metrics;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');

	config = args.config;
	dsname = args.dsname;
	error = null;

	if (error === null) {
		dsconfig = config.datasourceGet(args.dsname);
		if (dsconfig === null)
			return (new VError('unknown datasource: "%s"', dsname));
	}

	metrics = metricsForIndex(args);
	if (metrics.length === 0)
		return (new VError('no metrics defined for dataset "%s"',
		    dsname));

	return (mod_dragnet_impl.indexConfig({
	    'datasource': {
	        'backend': dsconfig.ds_backend,
		'datapath': dsconfig.ds_backend_config.path
	    },
	    'metrics': metrics,
	    'user': 'nobody',
	    'mtime': new Date()
	}));
}

/*
 * Do an index-read, which reads data points for all metrics configured for an
 * index, tagged with a __dn_metric field, and saves sqlite index files for
 * them.
 *
 *     log		bunyan-style logger
 *
 *     config		dragnet configuration
 *
 *     indexConfig	index configuration, which overrides metrics stored in
 *     			the configuration
 *
 *     interval		index interval
 *
 *     dsname		datasource name
 */
function indexRead(args, callback)
{
	var error, config, dsname, log, ds;
	var metrics;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');
	mod_assertplus.string(args.interval, 'args.interval');

	config = args.config;
	dsname = args.dsname;
	log = args.log;
	error = null;

	if (error === null) {
		ds = datasourceForName({
		    'log': log,
		    'config': config,
		    'dsname': dsname
		});
		if (ds instanceof Error)
			error = ds;
	}

	if (error === null)
		metrics = metricsForIndex(args);

	if (metrics.length === 0)
		error = new VError(
		    'no metrics defined for dataset "%s"', dsname);

	if (error !== null) {
		setImmediate(callback, error);
		return (null);
	}

	return (ds.indexRead({
	    'interval': args.interval,
	    'metrics': metrics
	}, callback));
}

/*
 * Do an index-scan, which is a scan that emits data points for all metrics
 * configured for an index, tagged with a __dn_metric field.
 *
 *     log		bunyan-style logger
 *
 *     config		dragnet configuration
 *
 *     indexConfig	index configuration, which overrides metrics stored in
 *     			the configuration
 *
 *     dsname		datasource name
 *
 *     interval		indexing interval (used to tack on __dn_ts field)
 *
 *     [timeAfter]	don't scan data with timestamp before this
 *
 *     [timeBefore]	don't scan data with timestamp after this
 */
function indexScan(args)
{
	var error, config, dsname, log, ds;
	var metrics;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');
	mod_assertplus.string(args.interval, 'args.interval');
	mod_assertplus.optionalObject(args.timeAfter, 'args.timeAfter');
	mod_assertplus.optionalObject(args.timeBefore, 'args.timeBefore');

	config = args.config;
	dsname = args.dsname;
	log = args.log;
	error = null;

	if (args.timeBefore !== null && args.timeAfter !== null &&
	    args.timeBefore.getTime() < args.timeAfter.getTime())
		error = new VError(
		    '"before" time cannot be before "after" time');

	if (error === null) {
		ds = datasourceForName({
		    'log': log,
		    'config': config,
		    'dsname': dsname
		});
		if (ds instanceof Error)
			error = ds;
	}

	if (error === null)
		metrics = metricsForIndex(args);

	if (metrics.length === 0)
		error = new VError(
		    'no metrics defined for dataset "%s"', dsname);

	if (error !== null)
		return (mod_dragnet_impl.asyncError(error));

	return (ds.indexScan({
	    'metrics': metrics,
	    'interval': args.interval,
	    'timeAfter': args.timeAfter,
	    'timeBefore': args.timeBefore
	}));
}

/*
 * Construct a list of metrics from the index configuration or the metrics
 * configured for the named dataset.
 */
function metricsForIndex(args)
{
	var config, dsname, metrics;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.config, 'args.config');
	mod_assertplus.string(args.dsname, 'args.dsname');
	mod_assertplus.optionalObject(args.indexConfig, 'args.indexConfig');

	config = args.config;
	dsname = args.dsname;
	metrics = [];
	if (!args.indexConfig) {
		config.datasourceListMetrics(dsname,
		    function (metname, mconfig) {
			metrics.push(mconfig);
		    });
	} else {
		args.indexConfig.metrics.forEach(function (mserialized) {
			metrics.push(
			    mod_dragnet_impl.metricDeserialize(mserialized));
		});
	}

	return (metrics);
}
