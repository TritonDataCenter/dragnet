/*
 * lib/dragnet.js: dragnet library interface
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var VError = require('verror');

/* Public interface */
exports.indexLoad = indexLoad;
exports.indexAggregator = indexAggregator;
exports.queryLoad = queryLoad;

/*
 * This is more of a struct than a class.  Its fields are used directly by
 * consuming code.  The constructor is private, and should only be invoked with
 * a validated index configuration.
 */
function IndexConfig(args)
{
	var rawindex, byname;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.rawindex, 'args.rawindex');
	mod_assertplus.arrayOfObject(args.columns, 'args.columns');

	rawindex = args.rawindex;

	/*
	 * The following fields are exactly the same as the corresponding
	 * top-level properties in the "index" schema, with optional fields
	 * being "null" if unspecified.
	 */
	this.ic_name = rawindex.name;
	this.ic_filter = rawindex.filter || null;

	/*
	 * Columns are also just as they appear in the schema, except that the
	 * string shorthand is expanded into an object so that consumers can
	 * always assume that canonical form.  Besides having the columns in
	 * order, it's useful to have them indexed by name.
	 */
	this.ic_columns = mod_jsprim.deepCopy(args.columns);
	this.ic_columns_byname = byname = {};
	this.ic_columns.forEach(function (col) { byname[col.name] = col; });

	/*
	 * XXX removed: ic_krillfilter, ic_filterstream, ic_datastore,
	 * ic_format, ic_formatstream, ic_fsroot
	 */
}

/*
 * Validate the given index configuration, and load helper objects used to work
 * with this index.  On success, returns an IndexConfig object.  On failure,
 * return an Error describing the problem.
 */
function indexLoad(args)
{
	var rawindex, filter, columns, indexconf;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');

	/*
	 * Parse and validate the filter.
	 * XXX validate basic structure first (fields present and with the
	 * correct types)
	 * XXX to be consistent with the query object, we shouldn't actually
	 * save the predicate or generate the filterstream here.
	 */
	rawindex = args.index;
	filter = rawindex.filter;
	if (filter) {
		try {
			mod_krill.createPredicate(filter);
		} catch (ex) {
			return (new VError(ex, 'invalid filter'));
		}
	}

	/*
	 * Expand and validate the index column definitions.
	 */
	columns = parseFields(rawindex.columns);
	if (columns instanceof Error)
		return (columns);

	indexconf = new IndexConfig({
	    'rawindex': rawindex,
	    'columns': columns
	});

	return (indexconf);
}

/*
 * Return a node-skinner aggregator stream with the specified breakdowns.
 *
 *     index		an index, as generated with indexLoad()
 *
 *     breakdowns	array of strings identifying fields to break down by.
 *     			See "decomps" argument to node-skinner aggregator.
 *     			Bucketizers are specified automatically based on numeric
 *     			fields in the index configuration.  (It's not currently
 *     			possible to specify numeric breakdowns with non-indexed
 *     			fields, even though the implementation supports that.
 *     			If that's useful, we'd need a way to specify that here.)
 *
 *     allowExternal	allow breakdowns on fields not specified in the index.
 *     (default: false) This is useful for the query implementation that scans
 *     			raw data rather than only indexed data.
 *
 *     options		extra node-skinner options (optional)
 *
 * An Error may be returned instead of a stream if there's a validation error.
 */
function indexAggregator(args)
{
	var index, decomps, bucketizer, bucketizers, stream, skinneroptions;
	var field, i, fieldconf;

	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.arrayOfString(args.breakdowns, 'args.breakdowns');
	mod_assertplus.optionalBool(args.allowExternal, 'args.allowExternal');
	mod_assertplus.optionalObject(args.options, 'args.options');

	index = args.index;
	decomps = [];
	bucketizer = null;
	bucketizers = {};

	for (i = 0; i < args.breakdowns.length; i++) {
		field = args.breakdowns[i];
		if (!index.ic_columns_byname.hasOwnProperty(field)) {
			if (!args.allowExternal) {
				return (new VError('breakdown %d ("%s"): ' +
				    'field is not indexed', i, field));
			}

			/*
			 * With no configuration, we're forced to assume that
			 * it's a discrete field rather than a numeric one.
			 */
			decomps.push(field);
			continue;
		}

		decomps.push(field);
		fieldconf = index.ic_columns_byname[field];
		if (!fieldconf.hasOwnProperty('aggr'))
			continue;

		if (fieldconf.aggr != 'quantize') {
			return (new VError('breakdown %d ("%s"): ' +
			    'unsupported aggregation type: "%s"',
			    i, field, fieldconf.aggr));
		}

		if (i != args.breakdowns.length - 1) {
			return (new VError('breakdown %d ("%s"): ' +
			    'quantized breakdowns must be last', i, field));
		}

		bucketizer = mod_skinner.makeP2Bucketizer();
		bucketizers[fieldconf.field] = bucketizer;
	}

	skinneroptions = {};
	if (args.options) {
		for (i in args.options)
			skinneroptions[i] = args.options[i];
	}
	skinneroptions['bucketizers'] = bucketizers;
	skinneroptions['decomps'] = decomps;
	skinneroptions['ordinalBuckets'] = true;
	stream = mod_skinner.createAggregator(skinneroptions);
	return ({
	    'stream': stream,
	    'bucketizer': bucketizer || null
	});
}

/*
 * Like IndexConfig, this is a struct-like class that represents the immutable
 * parameters of a specific query.
 */
function QueryConfig(args)
{
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.optionalObject(args.filter, 'args.filter');
	mod_assertplus.arrayOfObject(args.breakdowns, 'args.breakdowns');

	this.qc_filter = args.filter || null;
	this.qc_breakdowns = mod_jsprim.deepCopy(args.breakdowns);
	this.qc_fieldsbyname = {};
	this.qc_bucketizers = {};

	this.qc_breakdowns.forEach(function (fieldconf) {
		self.qc_fieldsbyname[fieldconf.name] = fieldconf;

		if (!fieldconf.hasOwnProperty('aggr'))
			return;

		mod_assertplus.equal(fieldconf.aggr, 'quantize');
		self.qc_bucketizers[fieldconf.name] =
		    mod_skinner.makeP2Bucketizer();
	});
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
 *     [index]		An index, as constructed with indexLoad.  If specified,
 *			all fields of "breakdowns" must be simple names that
 *			refer to columns from the index.
 */
function queryLoad(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.optionalObject(args.query.filter, 'args.query.filter');
	mod_assertplus.ok(Array.isArray(args.query.breakdowns));
	mod_assertplus.optionalObject(args.index, 'args.index');

	var filter, ret;

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

	ret = parseFields(args.query.breakdowns, args.index);
	if (ret instanceof Error)
		return (new VError(ret, 'invalid query'));

	return (new QueryConfig({
	    'filter': filter,
	    'breakdowns': ret
	}));
}

function parseFields(inputs, index)
{
	var fields, i, b, ret;

	fields = new Array(inputs.length);
	for (i = 0; i < inputs.length; i++) {
		b = inputs[i];
		ret = parseField(b, index);
		if (ret instanceof Error) {
			return (new VError(ret,
			    'field "%d" ("%s") is invalid', i, b));
		}

		fields[i] = ret;
	}

	return (fields);
}

function parseField(b, index)
{
	var i, j, kvpairs, rv;

	if (index) {
		if (typeof (b) != 'string')
			return (new Error('expected string'));

		if (!index.ic_columns_byname.hasOwnProperty(b))
			return (new VError('not found in index'));

		return (index.ic_columns_byname[b]);
	}

	if (typeof (b) != 'string') {
		if (b.hasOwnProperty('aggr') && b['aggr'] != 'quantize')
			return (new VError(
			    'unsupported "aggr": "%s"', b['aggr']));

		return (b);
	}

	i = b.indexOf('[');
	j = b.indexOf(']');
	if ((i == -1 && j != -1) ||
	    (i != -1 && j != b.length - 1))
		return (new VError('invalid field name'));

	rv = {};
	if (i == -1) {
		rv['name'] = b;
	} else {
		rv['name'] = b.substr(0, i);
		kvpairs = b.substr(i + 1, j - (i + 1)).split(',');
		kvpairs.forEach(function (kvpair) {
			var parts = kvpair.split('=');
			if (parts[0].trim().length === 0)
				return;
			rv[parts[0]] = parts[1] || '';
		});
	}

	if (!rv.hasOwnProperty('field'))
		rv['field'] = rv['name'];

	return (rv);
}
