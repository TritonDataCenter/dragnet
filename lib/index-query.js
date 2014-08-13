/*
 * lib/index-query.js: execute a query using an index
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
var mod_vstream = require('vstream');
var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

var mod_dragnet_impl = require('./dragnet-impl');

module.exports = IndexQuerier;

var DB_VERSION = '~2';

/*
 * Arguments include:
 *
 *     log		bunyan-style logger
 *
 *     filename		index file name
 */
function IndexQuerier(args)
{
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.filename, 'args.filename');

	this.qi_log = args.log;
	this.qi_dbfilename = args.filename;
	this.qi_allowed_versions = DB_VERSION;

	this.qi_config = null;
	this.qi_metrics = null;

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

mod_util.inherits(IndexQuerier, mod_events.EventEmitter);

IndexQuerier.prototype.loadConfig = function ()
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

	barrier.start('dragnet_metrics');
	this.qi_db.all('SELECT * FROM dragnet_metrics', function (err, rows) {
		barrier.done('dragnet_metrics');

		if (error !== null)
			return;

		if (err) {
			error = err;
			return;
		}

		self.qi_metrics = rows.map(function (row, i) {
			var filter, params;

			try {
				filter = row.filter === null ? null :
				    JSON.parse(row.filter);
			} catch (ex) {
				if (error === null)
					error = new VError(ex,
					    'failed to parse filter ' +
					    'for metric "%s"', row.label);
			}

			try {
				params = row.params === null ? {} :
				    JSON.parse(row.params);
			} catch (ex) {
				if (error === null)
					error = new VError(ex,
					    'failed to parse params ' +
					    'for metric "%s"', row.label);
			}

			return ({
			    'qm_id': row.id,
			    'qm_label': row.label,
			    'qm_filter': filter,
			    'qm_params': params,
			    'qm_filter_raw': row.filter
			});
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

IndexQuerier.prototype.config = function ()
{
	return (this.qi_config);
};

/*
 * Given a query, return the name of the table in this index which should be
 * used to serve the query.  If none, return an error.
 */
IndexQuerier.prototype.findMetric = function (query)
{
	var met, filter_raw, pred, fields_needed, fields_have;
	var mi, qf, okay;

	if (query.qc_filter !== null)
		filter_raw = JSON.stringify(query.qc_filter);

	/*
	 * Use a linear scan through the metrics provided by this database to
	 * find the first one that will work.
	 */
	for (mi = 0; mi < this.qi_metrics.length; mi++) {
		met = this.qi_metrics[mi];

		if (met.qm_filter !== null) {
			/*
			 * If the metric has a filter applied to it, and the
			 * query doesn't, then we can't use this metric because
			 * it may contain only a subset of the data we need.
			 */
			if (query.qc_filter === null)
				continue;

			/*
			 * Similarly, if the metric has a filter and the query
			 * has one but it does not match exactly, then skip this
			 * metric.  It's possible that the metric's query is a
			 * superset of the filter's query, in which case we may
			 * reject queries here that we could actually serve, but
			 * we don't believe this will be a common case.
			 */
			if (met.qm_filter_raw != filter_raw)
				continue;
		}

		/*
		 * Construct the set of fields needed for this query if it were
		 * to be provided by this metric, as well as the set of fields
		 * provided by this metric.
		 */
		fields_needed = {};
		fields_have = {};
		if (query.qc_filter !== null && met.qm_filter === null) {
			if (pred === undefined) {
				pred = mod_krill.createPredicate(
				    query.qc_filter);
				if (pred instanceof Error) {
					return (new VError(pred, 'failed to ' +
					    'create predicate for query'));
				}
			}

			pred.fields().forEach(function (f) {
				fields_needed[f] = true;
			});
		} else {
			mod_assertplus.ok(query.qc_filter === null ||
			    filter_raw == met.qm_filter_raw);
		}

		query.qc_breakdowns.forEach(
		    function (b) { fields_needed[b.name] = b; });
		met.qm_params.forEach(
		    function (b) { fields_have[b.name] = b; });

		/*
		 * Now compare what's needed to what's provided.
		 */
		okay = true;
		for (qf in fields_needed) {
			if (!fields_have.hasOwnProperty(qf)) {
				this.qi_log.trace('ignoring metric %s ' +
				    '(missing field %s)', met.qm_label, qf);
				okay = false;
				break;
			}
		}

		if (okay) {
			this.qi_log.debug('using metric %s', met.qm_label);
			return ({
			    'table': 'dragnet_index_' + met.qm_id,
			    'ignore_filter': met.qm_filter !== null
			});
		}
	}

	return (new VError('no metrics available to serve query'));
};

/*
 * Returns a stream that will emit the results of this query as node-skinner
 * data points.
 */
IndexQuerier.prototype.run = function (query)
{
	var self = this;
	var table, filter, whenfilter, columns, groupby, stmt, sql, stream;
	var qfilter;

	mod_assertplus.ok(this.qi_config !== null && this.qi_metrics !== null,
	    'IndexQuerier is not initialized');

	table = this.findMetric(query);
	if (table instanceof Error)
		return (mod_dragnet_impl.asyncError(table));

	/*
	 * Set up an aggregator for the results.  It's kind of annoying that we
	 * have to do this, since the results will usually be properly
	 * aggregated already.  The problem is that the database will return a
	 * separate rows for each entry in a numeric distribution, while our
	 * caller expects these to be aggregated together.
	 *
	 * On the plus side, we want to provide a stream interface that emits
	 * skinner points anyway, so this is a convenient way to do that.
	 * There's no real flow-control here, but the nature of this operation
	 * is that we're accumulating all the data that we're going to emit
	 * in-memory anyway.
	 */
	stream = mod_dragnet_impl.queryAggrStream({
	    'query': query,
	    'options': {
		'resultsAsPoints': true
	    }
	});
	stream = mod_vstream.wrapTransform(stream, 'Index Aggregator');

	/* Build WHERE clause. */
	whenfilter = mod_dragnet_impl.queryTimeBoundsFilter(query);
	qfilter = table.ignore_filter ? null : query.qc_filter;

	if (qfilter !== null && whenfilter !== null)
		filter = { 'and': [
		    mod_jsprim.deepCopy(qfilter), whenfilter ] };
	else if (whenfilter !== null)
		filter = whenfilter;
	else if (qfilter !== null)
		filter = mod_jsprim.deepCopy(qfilter);
	else
		filter = {};
	this.escapeFilter(filter);

	if (query.qc_breakdowns) {
		groupby = query.qc_breakdowns.filter(function (b) {
			return (!b.hasOwnProperty('date') || b.field == b.name);
		}).map(function (b) {
			return (self.sqlite3Escape(b.name));
		});
		columns = groupby.slice(0);
	} else {
		groupby = [];
		columns = [];
	}

	columns.push('SUM(value) as value');

	sql = 'SELECT ';
	sql += (columns.join(','));
	sql += ' from ' + table.table + ' ';
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
		stmt.each(function (err, row) {
			if (err) {
				/*
				 * XXX What are we supposed to do with the
				 * per-row callback, and what are the resulting
				 * semantics of the end-of-rows callback?
				 */
				self.qi_log.warn(err, 'error retrieving row');
				return;
			}

			self.qi_log.trace('query result', err, row);
			stream.write(self.deserializeRow(query, row));
		}, function (err) {
			stream.end();

			if (err)
				err = new VError(err, 'retrieving rows');
			stepcb(err);
		});
	    },
	    function (stepcb) {
		self.qi_log.trace('finalizing statement');
		stmt.finalize(stepcb);
	    }
	], function (err) {
		if (err) {
			err = new VError(err, 'executing query "%s"', sql);
			stream.emit('error', err);
		}
	});

	return (stream);
};

IndexQuerier.prototype.deserializeRow = function (query, row)
{
	var breakdowns, point;
	var self = this;

	/*
	 * The SQL standard defines the SUM() of an empty set as NULL, not zero.
	 * If we find NULL here, replace it with zero.
	 */
	if (row.value === null)
		row.value = 0;

	point = {
	    'fields': {},
	    'value': row.value
	};
	breakdowns = query.qc_breakdowns;
	breakdowns.forEach(function (field, i) {
		var val = row[self.sqlite3Escape(field.field)];
		point['fields'][field.name] = val;
	});

	return (point);
};

/* XXX commonize with index-scan.js */
IndexQuerier.prototype.sqlite3Escape = function (str)
{
	return (str.replace(/\./g, '_'));
};

/* XXX internal knowledge */
IndexQuerier.prototype.escapeFilter = function (filter)
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

/* XXX internal knowledge */
IndexQuerier.prototype.toSqlString = function (filter)
{
	var self = this;

	if (mod_jsprim.isEmpty(filter))
		return ('1');

	if (filter['and']) {
		return (filter['and'].map(function (clause) {
			return (sprintf('(%s)', self.toSqlString(clause)));
		}).join(' AND '));
	}

	if (filter['or']) {
		return (filter['and'].map(function (clause) {
			return (sprintf('(%s)', self.toSqlString(clause)));
		}).join(' OR '));
	}

	return (mod_krill.createPredicate(filter).toCStyleString());
};

IndexQuerier.prototype.filterWhere = function (filter)
{
	return ('WHERE ' + this.toSqlString(filter) + ' ');
};
