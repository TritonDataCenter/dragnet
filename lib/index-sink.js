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
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var mod_vasync = require('vasync');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/* XXX should this be a dragnet-impl.js instead? */
var mod_dragnet = require('./dragnet');

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
 * index		index configuration
 *
 * filename		file name to which to save the index
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 */
function IndexSink(args)
{
	var streamoptions, dbfilename, dirname;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.filename, 'args.filename');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true });
	mod_stream.Transform.call(this, streamoptions);

	this.is_log = args.log;
	this.is_index = args.index;
	this.is_dbtmpfilename = dbfilename = args.filename + '.' + process.pid;
	this.is_dbfilename = args.filename;

	this.is_ntransformed = 0;
	this.is_db = null;
	this.is_insert = null;
	this.is_buffered = [];

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
	this.doFlush(callback);
};

IndexSink.prototype.doInitDb = function ()
{
	var self = this;
	var tables, configpairs, columnpairs;

	tables = {
	    'dragnet_config': {
		'key': 'varchar(128) primary key',
		'value': 'varchar(128)'
	    },
	    'dragnet_columns': {
	        'name': 'varchar(128) primary key',
		'field': 'varchar(128)',
		'aggr': 'varchar(128)'
	    },
	    'dragnet_index': {}
	};

	configpairs = [];
	configpairs.push([ 'version', '1.0.0' ]);
	configpairs.push([ 'index_name', this.is_index.ic_name ]);
	configpairs.push([ 'index_filter',
	    JSON.stringify(this.is_index.ic_filter) ]);

	columnpairs = this.is_index.ic_columns.map(function (col) {
		return ([ col.name, col.field, col.aggr || '' ]);
	});

	this.is_index.ic_columns.forEach(function (col) {
		tables['dragnet_index'][self.sqlite3Escape(col.name)] =
		    col.aggr ? 'integer' : 'varchar(128)';
	});
	tables['dragnet_index']['value'] = 'integer';

	this.is_log.debug('initializing database');
	mod_vasync.waterfall([
	    this.doCreateTable.bind(this, 'dragnet_config',
	        tables['dragnet_config']),
	    this.doCreateTable.bind(this, 'dragnet_columns',
	        tables['dragnet_columns']),
	    this.doCreateTable.bind(this, 'dragnet_index',
	        tables['dragnet_index']),
	    this.doInsertTable.bind(this, 'dragnet_config', configpairs),
	    this.doInsertTable.bind(this, 'dragnet_columns', columnpairs),
	    function prepare(callback) {
		self.is_insert = self.is_db.prepare(
		    'INSERT INTO dragnet_index VALUES (' +
		    (Object.keys(tables['dragnet_index']).map(
		    function (c) { return ('?'); }).join(', ')) + ')',
		    callback);

		var buffered = self.is_buffered;
		self.is_buffered = null;
		buffered.forEach(function (r) { self.doIndex(r[0], r[1]); });
		self.is_log.debug('replayed %d buffered records',
		    buffered.length);
	    }
	], function (err) {
		if (err) {
			err = new VError(err, 'initializing database');
			self.is_log.error(err);
			self.emit('error', err);
			return;
		}

		self.is_log.info('database initialized');
	});
};

IndexSink.prototype.sqlite3Escape = function (str)
{
	return (str.replace(/\./g, '_'));
};

IndexSink.prototype.doIndex = function (chunk, callback)
{
	var row;

	this.is_log.debug('indexing', chunk);

	if (this.is_insert === null) {
		/* Not yet ready.  Buffer requests. */
		this.is_buffered.push([ chunk, callback ]);
		return;
	}

	this.is_ntransformed++;
	row = new Array(this.is_index.ic_columns.length + 1);

	this.is_index.ic_columns.forEach(function (c, i) {
		mod_assertplus.ok(chunk.fields.hasOwnProperty(c.name));
		row[i] = chunk.fields[c.name];
	});

	row[row.length - 1] = chunk.value;
	this.is_insert.run(row, callback);
};

IndexSink.prototype.doFlush = function (callback)
{
	var self = this;

	this.is_log.debug('flush');

	mod_vasync.waterfall([
	    function (stepcb) {
		self.is_insert.finalize(stepcb);
	    },
	    function (stepcb) {
		self.is_db.close(function (err) {
			if (err)
				err = new VError(err, 'error closing database');
			stepcb(err);
		});
	    },
	    function (stepcb) {
		self.is_log.debug('renaming');
		mod_fs.rename(self.is_dbtmpfilename, self.is_dbfilename,
		    function (err) {
			if (err)
				err = new VError(err, 'renaming "%s"',
				    self.is_dbtmpfilename);
			stepcb(err);
		    });
	    }
	], function (err) {
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

IndexSink.prototype.stats = function ()
{
	return ({ 'ninputs': this.is_ntransformed });
};
