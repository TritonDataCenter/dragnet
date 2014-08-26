/*
 * lib/index-sink.js: stream that receives skinner records and generates a
 * sqlite3 index.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_mkdirp = require('mkdirp');
var mod_path = require('path');
var mod_skinner = require('skinner');
var mod_sqlite3 = require('sqlite3');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_vstream = require('vstream');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

var mod_dragnet_impl = require('./dragnet-impl');
var mod_streamutil = require('./stream-util');

/* Public interface */
module.exports = IndexSink;

/*
 * Object-mode Writable stream:
 *
 *     input (object-mode):  plain JavaScript objects representing *aggregated*
 *         skinner data points.  There must be no duplicates.
 *
 * log			bunyan-style logger
 *
 * metrics		array of metric configuration objects
 *
 * filename		file name to which to save the index
 *
 * [config]		extra configuration properties
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 */
function IndexSink(args)
{
	var streamoptions, dbfilename, dirname;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.arrayOfObject(args.metrics, 'args.metrics');
	mod_assertplus.string(args.filename, 'args.filename');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');
	mod_assertplus.optionalObject(args.config, 'args.config');

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true },
	    { 'highWaterMark': 512 });
	mod_stream.Transform.call(this, streamoptions);
	mod_vstream.wrapTransform(this);

	this.is_log = args.log;
	this.is_metrics = args.metrics;
	this.is_dbtmpfilename = dbfilename = args.filename + '.' + process.pid;
	this.is_dbfilename = args.filename;
	this.is_config = mod_jsprim.deepCopy(args.config || {});

	this.is_db = null;
	this.is_insert = null;
	this.is_ready = false;
	this.is_buffered = [];
	this.is_flushcb = null;

	dirname = mod_path.dirname(dbfilename);
	mod_mkdirp(dirname, function (err) {
		if (err) {
			err = new VError(err, 'failed to create "%s"', dirname);
			self.is_log.error(err, 'initialization error');
			self.emit('error', err);
			return;
		}

		self.is_db = new mod_sqlite3.Database(dbfilename);

		self.is_db.on('open', function () {
			self.is_log.info('opened database "%s"', dbfilename);
			self.doInitDb();
		});

		self.is_db.on('error', function (dberr) {
			self.is_log.error(dberr, 'fatal database error');
			self.emit('error', new VError(dberr, 'database error'));
		});
	});
}

mod_util.inherits(IndexSink, mod_stream.Transform);

IndexSink.prototype._transform = function (chunk, _, callback)
{
	this.doIndex(chunk, callback);
};

IndexSink.prototype._flush = function (callback)
{
	if (!this.is_ready) {
	        /* We're not initialized yet. */
	        this.is_flushcb = callback;
	        this.is_log.debug('flush called, but not initialized yet');
	        return;
	}

	this.doFlush(callback);
};

IndexSink.prototype.doInitDb = function ()
{
	var self = this;
	var tables, configpairs, metricrows, k, stages;

	tables = {
	    'dragnet_config': {
		'key': 'varchar(128) primary key',
		'value': 'varchar(128)'
	    },
	    'dragnet_metrics': {
		'id': 'integer',
		'label': 'varchar(64)',
		'filter': 'varchar(1024)',
		'params': 'varchar(1024)'
	    }
	};

	configpairs = [];
	configpairs.push([ 'version', '2.0.0' ]);

	for (k in this.is_config) {
		mod_assertplus.ok(k != 'version');
		configpairs.push([ k, this.is_config[k] ]);
	}

	metricrows = [];
	this.is_insert = new Array(this.is_metrics.length);
	this.is_metrics.forEach(function (m, i) {
		var row, ms, tblname;

		ms = mod_dragnet_impl.metricSerialize(m, true);
		row = [
		    i,
		    m.m_name,
		    JSON.stringify(m.m_filter),
		    JSON.stringify(ms.breakdowns)
		];
		metricrows.push(row);

		tblname = 'dragnet_index_' + i;
		tables[tblname] = {};
		m.m_breakdowns.forEach(function (b) {
			tables[tblname][self.sqlite3Escape(b.b_name)] =
			    b.b_aggr ? 'integer' : 'varchar(128)';
		});
		tables[tblname]['value'] = 'integer';
	});

	/*
	 * Disable fsync.  The caller is responsible for ensuring that the file
	 * is sync'ed if they care.
	 */
	stages = [
	    function nosync(callback) {
		self.is_db.exec('pragma synchronous = off;', function (err) {
			if (err)
				callback(new VError(err, 'disable sync'));
			else
				callback();
		});
	    }
	];

	mod_jsprim.forEachKey(tables, function (tblname, tblconfig) {
		stages.push(function (stepcb) {
			self.doCreateTable(tblname, tblconfig, stepcb);
		});
	});

	stages.push(function (stepcb) {
		self.doInsertTable('dragnet_config', configpairs, stepcb);
	});

	stages.push(function (stepcb) {
		self.doInsertTable('dragnet_metrics', metricrows, stepcb);
	});

	this.is_metrics.forEach(function (m, i) {
		stages.push(function (stepcb) {
			var tblname = 'dragnet_index_' + i;
			self.is_insert[i] = self.is_db.prepare(
			    'INSERT INTO ' + tblname + ' VALUES (' +
			    (Object.keys(tables[tblname]).map(
			    function (c) { return ('?'); }).join(', ')) + ')',
			    stepcb);
		});
	});

	stages.push(function prepare(stepcb) {
		var buffered = self.is_buffered;
		self.is_ready = true;
		self.is_buffered = null;
		buffered.forEach(function (r) { self.doIndex(r[0], r[1]); });
		self.is_log.debug('replayed %d buffered records',
		    buffered.length);
	});

	this.is_log.debug('initializing database');
	mod_vasync.waterfall(stages, function (err) {
		if (err) {
			err = new VError(err, 'initializing database');
			self.is_log.error(err);
			self.emit('error', err);
			return;
		}

		self.is_log.info('database initialized');
	        if (self.is_flushcb !== null) {
	                var c = self.is_flushcb;
	                self.is_flushcb = null;
	                self.doFlush(c);
	        }
	});
};

IndexSink.prototype.sqlite3Escape = function (str)
{
	return (str.replace(/[\.-]/g, '_'));
};

IndexSink.prototype.doIndex = function (chunk, callback)
{
	var row, mi, m;

	this.is_log.debug('indexing', chunk);

	if (!this.is_ready) {
		/* Not yet ready.  Buffer requests. */
		this.is_buffered.push([ chunk, callback ]);
		return;
	}

	mi = chunk.fields['__dn_metric'];
	mod_assertplus.equal(typeof (mi), 'number');
	mod_assertplus.ok(mi >= 0 && mi < this.is_metrics.length);
	m = this.is_metrics[mi];
	row = new Array(m.m_breakdowns.length + 1);

	m.m_breakdowns.forEach(function (b, i) {
		mod_assertplus.ok(chunk.fields.hasOwnProperty(b.b_name));
		row[i] = chunk.fields[b.b_name];
	});

	row[row.length - 1] = chunk.value;
	this.is_insert[mi].run(row, callback);
};

IndexSink.prototype.doFlush = function (callback)
{
	var self = this;

	this.is_log.debug('flush');

	mod_vasync.waterfall([
	    function (stepcb) {
	        self.is_log.debug('flush: finalize');
		mod_vasync.forEachParallel({
		    'func': function (stmt, pcb) { stmt.finalize(pcb); },
		    'inputs': self.is_insert
		}, function (err) {
			stepcb(err);
		});
	    },
	    function (stepcb) {
	        self.is_log.debug('flush: close');
		self.is_db.close(function (err) {
			if (err)
				err = new VError(err, 'error closing database');
			stepcb(err);
		});
	    },
	    function (stepcb) {
		self.is_log.debug('flush: rename');
		mod_fs.rename(self.is_dbtmpfilename, self.is_dbfilename,
		    function (err) {
			if (err)
				err = new VError(err, 'renaming "%s"',
				    self.is_dbtmpfilename);
			stepcb(err);
		    });
	    }
	], function (err) {
	        self.is_log.debug('flush: done');
		if (err)
			self.is_log.error(err);
		callback(err);
	});
};

IndexSink.prototype.doCreateTable = function (tablename, columns, callback)
{
	var sql = '';
	var collines;

	sql  = 'CREATE TABLE ' + tablename + '(\n';
	collines = [];
	mod_jsprim.forEachKey(columns, function (key, value) {
		collines.push('    ' + key + ' ' + value);
	});
	sql += collines.join(',\n');
	sql += '\n);';

	this.is_log.debug({
	    'table': tablename,
	    'columns': Object.keys(columns)
	}, 'create table');
	this.is_db.exec(sql, callback);
};

IndexSink.prototype.doInsertTable = function (tablename, values, callback)
{
	var self = this;
	var sql, st;

	if (values.length === 0) {
		setImmediate(callback);
		return;
	}

	sql = 'INSERT INTO ' + tablename + ' VALUES (' +
	    values[0].map(function () { return ('?'); }).join(', ') + ')';
	this.is_log.debug('prepare', { 'sql': sql });
	st = this.is_db.prepare(sql, function (err) {
		if (err) {
			callback(new VError(
			    err, 'failed to prepare statement'));
			return;
		}

		mod_vasync.forEachPipeline({
		    'inputs': values,
		    'func': function (rowvalues, stepcb) {
			self.is_log.debug({
			    'table': tablename,
			    'values': rowvalues
			}, 'insert');
			st.run(rowvalues, stepcb);
		    }
		}, function (perr) {
			st.finalize(function () {
				callback(perr);
			});
		});
	});
};
