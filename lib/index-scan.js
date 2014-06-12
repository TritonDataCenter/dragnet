/*
 * lib/index-scan.js: execute a query over a stream of raw data
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_krill = require('krill');
var mod_skinner = require('skinner');
var mod_sqlite3 = require('sqlite3');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var mod_vasync = require('vasync');
var PipelineStream = require('./stream-pipe');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/* Public interface */
module.exports = IndexScanner;

/*
 * Object-mode Writable stream:
 *
 *     input (object-mode):  plain JavaScript objects representing records
 *
 * log			bunyan-style logger
 *
 * index		index configuration (see schema)
 *
 * [streamOptions]	options to pass through to Node.js Stream constructor
 *
 * XXX consider generalizing "time" to "primary key" and having the previous
 * stream in the pipeline normalize records to put the value in a known format
 * in a known field?
 */
function IndexScanner(args)
{
	var streamoptions, streams, stream;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.is_log = args.log;
	this.is_index = args.index;
	this.is_filter = null;
	this.is_ntransformed = 0;
	this.is_db = null;

	streams = [];
	if (this.is_index.ic_filterstream) {
		stream = this.is_index.ic_filterstream;
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		this.is_filter = stream;
		streams.push(stream);
	}

	/* XXX create predicate streams for timeStart, timeEnd */

	streams.push(mod_streamutil.transformStream({
	    'streamOptions': { 'objectMode': true },
	    'func': function (chunk, _, callback) {
		this.is_ntransformed++;
		this.push({
		    'fields': chunk,
		    'value': 1
		});
		callback();
	    }
	}));

	streams.push(mod_streamutil.transformStream({
	    'streamOptions': { 'objectMode': true },
	    'func': function (chunk, _, callback) {
		self.doIndex(chunk, callback);
	    },
	    'flush': function (callback) {
		self.doFlush(callback);
	    }
	}));

	streamoptions = mod_streamutil.streamOptions(
	    args.streamOptions, { 'objectMode': true });
	PipelineStream.call(this, {
	    'streams': streams,
	    'streamOptions': streamoptions
	});

	/* XXX */
	var dbfilename = './tmpdb.' + process.pid + '.sqlite3';
	this.is_db = new mod_sqlite3.Database(dbfilename);
	this.is_db.on('open', function () {
		self.is_log.info('opened database "%s"', dbfilename);
		self.doInitDb();
	});

	this.is_db.on('error', function (err) {
		self.is_log.error(err, 'fatal database error');
		self.emit('error', new VError(err, 'database error'));
	});
}

mod_util.inherits(IndexScanner, PipelineStream);

IndexScanner.prototype.doInitDb = function ()
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
	    }
	};

	configpairs = [];
	configpairs.push([ 'version', '1.0.0' ]);
	configpairs.push([ 'index_name', this.is_index.ic_name ]);
	configpairs.push([ 'index_format', this.is_index.ic_format ]);
	configpairs.push([ 'index_filter',
	    JSON.stringify(this.is_index.ic_filter) ]);

	columnpairs = this.is_index.ic_columns.map(function (col) {
		return ([ col.name, col.field, col.aggr || '' ]);
	});

	this.is_log.debug('initializing database');
	mod_vasync.waterfall([
	    this.doCreateTable.bind(this, 'dragnet_config',
	        tables['dragnet_config']),
	    this.doCreateTable.bind(this, 'dragnet_columns',
	        tables['dragnet_columns']),
	    this.doInsertTable.bind(this, 'dragnet_config', configpairs),
	    this.doInsertTable.bind(this, 'dragnet_columns', columnpairs)
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

IndexScanner.prototype.doIndex = function (chunk, callback)
{
	callback();
};

IndexScanner.prototype.doFlush = function (callback)
{
	var self = this;
	this.is_db.close(function (err) {
		if (err) {
			err = new VError(err, 'error closing database');
			self.is_log.error(err);
			callback(err);
		} else {
			callback();
		}
	});
};

IndexScanner.prototype.doCreateTable = function (tablename, columns, callback)
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

IndexScanner.prototype.doInsertTable = function (tablename, values, callback)
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
			callback(perr);
		});
	});
};

IndexScanner.prototype.stats = function ()
{
	var stats = {};
	var dsstats;

	if (this.is_filter !== null) {
		dsstats = this.is_filter.stats();
		stats.ninputs = dsstats.ninputs;
		stats.index_filter_nremoved = dsstats.nfilteredout;
		stats.index_filter_nerrors = dsstats.nerrors;
	} else {
		stats.index_filter_nremoved = 0;
		stats.index_filter_nerrors = 0;
	}

	if (!stats.hasOwnProperty('ninputs'))
		stats.ninputs = this.is_ntransformed;

	stats.aggr_nerr_nonnumeric = 0;
	return (stats);
};
