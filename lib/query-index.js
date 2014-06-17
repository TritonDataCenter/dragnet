/*
 * lib/query-index.js: execute a query using an index
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_semver = require('semver');
var mod_skinner = require('skinner');
var mod_sqlite3 = require('sqlite3');
var mod_util = require('util');
var mod_vasync = require('vasync');
var VError = require('verror');

/* XXX should this be a dragnet-impl.js instead? */
var mod_dragnet = require('./dragnet');

module.exports = QueryIndex;

/*
 * XXX should come from the framework
 */
var DB_FILENAME = 'tmpdb.sqlite';
var DB_VERSION = '~1';

/*
 * Arguments include:
 *
 *     log		bunyan-style logger
 *
 *     index		index configuration (see schema)
 */
function QueryIndex(args)
{
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');

	this.qi_log = args.log;
	this.qi_index = args.index;
	this.qi_dbfilename = DB_FILENAME;
	this.qi_allowed_versions = DB_VERSION;

	this.qi_config = null;
	this.qi_columns = null;

	this.qi_db = new mod_sqlite3.Database(
	    this.qi_dbfilename, mod_sqlite3.OPEN_READONLY);
	this.qi_db.on('open', function () {
		self.qi_log.info('opened database "%s"', self.qi_dbfilename);
		self.loadConfig();
	});

	this.qi_db.on('error', function (err) {
		self.qi_log.error(err, 'fatal database error');
		self.emit('error', new VError(err, 'database error'));
	});
}

mod_util.inherits(QueryIndex, mod_events.EventEmitter);

QueryIndex.prototype.loadConfig = function ()
{
	var self = this;
	var error = null;
	var barrier;

	barrier = mod_vasync.barrier();
	barrier.start('dragnet_config');
	this.qi_db.all('SELECT * FROM dragnet_config', function (err, rows) {
		barrier.done('dragnet_config');

		if (err) {
			error = err;
			return;
		}

		self.qi_config = {};
		rows.forEach(function (r) {
			self.qi_config[r.key] = r.value;
		});

		if (!self.qi_config.hasOwnProperty('version')) {
			error = new VError('index missing dragnet "version"');
		} else if (!mod_semver.satisfies(self.qi_config['version'],
		    self.qi_allowed_versions)) {
			error = new VError('unsupported index version: "%s"',
			    self.qi_config['version']);
		}
	});

	barrier.start('dragnet_columns');
	this.qi_db.all('SELECT * FROM dragnet_columns', function (err, rows) {
		barrier.done('dragnet_columns');

		if (error !== null)
			return;

		if (err) {
			error = err;
			return;
		}

		self.qi_columns = {};
		rows.forEach(function (r) {
			self.qi_columns[r.name] = r;
		});
	});

	barrier.on('drain', function () {
		if (error !== null) {
			self.emit('error', error);
		} else {
			self.emit('ready');
		}
	});
};

QueryIndex.prototype.createBucketizer = function (columnconfig)
{
	mod_assertplus.object(columnconfig);
	mod_assertplus.string(columnconfig.aggr);

	if (columnconfig.aggr !== 'quantize')
		return (null);

	return (mod_skinner.makeP2Bucketizer());
};

QueryIndex.prototype.run = function (query, callback)
{
	var self = this;
	var filter, columns, groupby, stmt, sql, rows;
	var skinnerconf, stream, bucketizer;

	mod_assertplus.ok(this.qi_config !== null && this.qi_columns !== null,
	    'QueryIndex is not initialized');

	/*
	 * Set up an aggregator for the results.  It's kind of annoying that we
	 * have to do this, since the results will usually be properly
	 * aggregated already.  The problem is that the database will return a
	 * separate rows for each entry in a numeric distribution, while our
	 * caller expects these to be aggregated together.
	 */
	skinnerconf = mod_dragnet.indexAggregator({
	    'index': this.qi_index,
	    'breakdowns': query.breakdowns,
	    'allowExternal': false
	});
	if (skinnerconf instanceof Error) {
		setImmediate(callback, skinnerconf);
		return;
	}
	stream = skinnerconf.stream;
	this.qi_bucketizer = bucketizer = skinnerconf.bucketizer;

	/* Build WHERE clause. */
	filter = mod_jsprim.deepCopy(query.filter || {});
	this.escapeFilter(filter);

	if (query.breakdowns) {
		groupby = query.breakdowns.map(
		    this.sqlite3Escape.bind(this));
		columns = query.breakdowns.map(
		    this.sqlite3Escape.bind(this));
	} else {
		groupby = [];
		columns = [];
	}

	columns.push('SUM(value) as value');

	sql = 'SELECT ';
	sql += (columns.join(','));
	sql += ' from dragnet_index ';
	sql += this.filterWhere(filter);
	if (groupby.length > 0)
		sql += 'GROUP BY ' + (groupby.join(','));

	mod_vasync.waterfall([
	    function (stepcb) {
		self.qi_log.trace('prepare SQL', sql);
		stmt = self.qi_db.prepare(sql, stepcb);
	    },
	    function (stepcb) {
		self.qi_log.trace('execute SQL', sql);
		stmt.all(stepcb);
	    },
	    function (results, stepcb) {
		self.qi_log.trace('query results', results.length);
		results.forEach(function (r) {
			stream.write(self.deserializeRow(query, r));
		});
		stmt.finalize(stepcb);
	    },
	    function (stepcb) {
		self.qi_log.trace('statement finalized');
		stream.on('data', function (chunk) {
			/* There should be only one datum. */
			rows = chunk;
		});
		stream.on('end', stepcb);
		stream.end();
	    }
	], function (err) {
		if (err)
			err = new VError(err, 'executing query "%s"', sql);
		callback(err, rows, bucketizer);
	});
};

QueryIndex.prototype.deserializeRow = function (query, row)
{
	var breakdowns = query.breakdowns || []; /* XXX */
	var point = {
	    'fields': {},
	    'value': row.value
	};
	var self = this;

	breakdowns.forEach(function (b, i) {
		var field = self.qi_index.ic_columns_byname[b];
		var val;
		mod_assertplus.ok(field);
		val = row[self.sqlite3Escape(field.field)];
		point['fields'][field.field] = val;
	});

	return (point);
};

/* XXX commonize with index-scan.js */
QueryIndex.prototype.sqlite3Escape = function (str)
{
	return (str.replace(/\./g, '_'));
};

/* XXX internal knowledge */
QueryIndex.prototype.escapeFilter = function (filter)
{
	if (mod_jsprim.isEmpty(filter))
		return;

	if (filter['and']) {
		filter['and'].forEach(this.escapeFilter.bind(this));
		return;
	}

	if (filter['or']) {
		filter['or'].forEach(this.escapeFilter.bind(this));
		return;
	}

	var key = Object.keys(filter)[0];
	filter[key][0] = this.sqlite3Escape(filter[key][0]);
};

QueryIndex.prototype.filterWhere = function (filter)
{
	var pred = mod_krill.createPredicate(filter);
	if (pred.trivial())
		return ('');

	return ('WHERE ' + pred.toCStyleString() + ' ');
};