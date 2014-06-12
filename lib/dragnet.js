/*
 * lib/dragnet.js: dragnet library interface
 */

var mod_assertplus = require('assert-plus');
var mod_krill = require('krill');
var VError = require('verror');

var mod_streamutil = require('./stream-util');
var JsonLineStream = require('../lib/format-json');
var LocalDataStore = require('../lib/datastore-local');

var schemaIndex = require('../schema/user-index');

/* Public interface */
exports.indexLoad = indexLoad;

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
	mod_assertplus.object(args.krillfilter, 'args.krillfilter');
	mod_assertplus.object(args.datastore, 'args.datastore');
	mod_assertplus.object(args.filterstream, 'args.filterstream');
	mod_assertplus.object(args.formatstream, 'args.formatstream');

	rawindex = args.rawindex;

	/*
	 * The following fields are exactly the same as the corresponding
	 * top-level properties in the "index" schema, with optional fields
	 * being "null" if unspecified.
	 */
	this.ic_name = rawindex.name;
	this.ic_fsroot = rawindex.fsroot;
	this.ic_format = rawindex.format;
	this.ic_filter = rawindex.filter || null;

	/*
	 * Columns are also just as they appear in the schema, except that the
	 * string shorthand is expanded into an object so that consumers can
	 * always assume that canonical form.  Besides having the columns in
	 * order, it's useful to have them indexed by name.
	 */
	this.ic_columns = rawindex.columns.map(function (col) {
		if (typeof (col) != 'string')
			return (col);

		return ({ 'name': col, 'field': col });
	});

	this.ic_columns_byname = byname = {};
	this.ic_columns.forEach(function (col) {
		byname[col.name] = col;
	});

	/*
	 * The following helper objects are created based on the configuration.
	 */
	this.ic_krillfilter = args.krillfilter || null;
	this.ic_filterstream = args.filterstream || null;
	this.ic_datastore = args.datastore;
	this.ic_formatstream = args.formatstream;
}

/*
 * Given a filename denoting an index configuration, read the file, parse the
 * configuration, validate it, and load helper objects used to work with this
 * index.  "callback" will be invoked with either an error (on failure) or an
 * object representing an index, with fields documented under IndexConfig above.
 */
function indexLoad(args, callback)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.filename, 'args.filename');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.func(callback, 'callback');

	mod_streamutil.readFileJson({
	    'filename': args.filename,
	    'schema': schemaIndex
	}, function (err, rawindex) {
		var predicate, filterstream;
		var datastore, formatstream;
		var indexconf;

		if (err) {
			callback(err);
			return;
		}

		/*
		 * At this point, the configuration has passed schema
		 * validation.  Now check that the predicate filter is valid.
		 */
		predicate = indexMakePredicate(rawindex);
		if (predicate instanceof Error) {
			callback(predicate);
			return;
		}

		if (predicate !== null) {
			filterstream = mod_krill.createPredicateStream(
			    { 'predicate': predicate });
		}

		/*
		 * Check a few additional constraints that should eventually be
		 * removed.
		 */
		if (!rawindex.hasOwnProperty('fsroot'))
			err = new VError('expected "fsroot"');
		else if (rawindex.format != 'json')
			err = new VError('only "json" format is supported');
		if (err) {
			callback(err);
			return;
		}

		/*
		 * Create the other helper objects and then create the
		 * IndexConfig to wrap it all up.
		 */
		datastore = new LocalDataStore({
		    'log': args.log,
		    'fsroot': rawindex.fsroot
		});

		formatstream = new JsonLineStream();

		indexconf = new IndexConfig({
		    'rawindex': rawindex,
		    'krillfilter': predicate,
		    'filterstream': filterstream,
		    'datastore': datastore,
		    'formatstream': formatstream
		});

		callback(null, indexconf);
	});
}

/*
 * Given a "raw" (but schema-validated) index configuration, return the
 * predicate representing the index's filter.  There are three possible
 * kinds of return values:
 *
 *     null		if there's no filter specified
 *
 *     an Error		if the filter is invalid
 *
 *     an object	if the filter is valid
 */
function indexMakePredicate(rawindex)
{
	var predicate;

	if (rawindex.filter === undefined)
		return (null);

	try {
		predicate = mod_krill.createPredicate(rawindex.filter);
	} catch (ex) {
		return (new VError(ex, 'failed to validate index filter'));
	}

	return (predicate);
}
