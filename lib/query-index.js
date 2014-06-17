/*
 * lib/query-index.js: execute a query using an index
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_sqlite3 = require('sqlite3');
var mod_util = require('util');
var mod_vasync = require('vasync');
var VError = require('verror');

module.exports = QueryIndex;

/*
 * XXX should come from the framework
 */
var DB_FILENAME = 'tmpdb.sqlite';

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
	this.qi_db = new mod_sqlite3.Database(
	    this.qi_dbfilename, mod_sqlite3.OPEN_READONLY);
	this.qi_db.on('open', function () {
		self.qi_log.info('opened database "%s"', self.qi_dbfilename);
		/* XXX read configuration */
	});

	this.qi_db.on('error', function (err) {
		self.qi_log.error(err, 'fatal database error');
		self.emit('error', new VError(err, 'database error'));
	});
}

mod_util.inherits(QueryIndex, mod_events.EventEmitter);

QueryIndex.prototype.run = function (query, callback)
{
	var self = this;
	var filter, columns, groupby, stmt, sql, rows;

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
		stmt = self.qi_db.prepare(sql, stepcb);
	    },
	    function (stepcb) {
		stmt.all(stepcb);
	    },
	    function (results, stepcb) {
		rows = results.map(self.deserializeRow.bind(self, query));
		stmt.finalize(stepcb);
	    }
	], function (err) {
		if (err)
			err = new VError(err, 'executing query "%s"', sql);
		callback(err, rows);
	});
};

QueryIndex.prototype.deserializeRow = function (query, row)
{
	var breakdowns = query.breakdowns || []; /* XXX */
	var rv = new Array(breakdowns.length + 1);
	var self = this;

	rv[rv.length - 1] = row.value;

	breakdowns.forEach(function (b, i) {
		var field = self.qi_index.ic_columns_byname[b];
		mod_assertplus.ok(field);
		rv[i] = row[self.sqlite3Escape(field.field)];
	});

	return (rv);
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
