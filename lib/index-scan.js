/*
 * lib/index-scan.js: execute a query over a stream of raw data
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
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

/* XXX should this be a dragnet-impl.js instead? */
var mod_dragnet = require('./dragnet');

/* Public interface */
module.exports = IndexScanner;

/*
 * XXX These should really be supplied by the framework.
 */
var DB_TMPFILENAME = 'tmpdb.' + process.pid + '.sqlite';
var DB_FILENAME = 'tmpdb.sqlite';

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
	var streamoptions, streams, stream, skinnerconf, dbfilename;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	this.is_log = args.log;
	this.is_index = args.index;
	this.is_filter = null;
	this.is_skinnerstream = null;
	this.is_ntransformed = 0;
	this.is_db = null;
	this.is_aggregator = null;
	this.is_insert = null;

	streams = [];
	if (this.is_index.ic_filterstream) {
		stream = this.is_index.ic_filterstream;
		stream.on('invalid_object',
		    this.emit.bind(this, 'invalid_object'));
		this.is_filter = stream;
		streams.push(stream);
	}

	/* XXX create predicate streams for timeStart, timeEnd */

	this.is_skinnerstream = mod_streamutil.skinnerStream();
	streams.push(this.is_skinnerstream);

	skinnerconf = mod_dragnet.indexAggregator({
	    'index': this.is_index,
	    'breakdowns': this.is_index.ic_columns.map(
	        function (c) { return (c.name); }),
	    'allowExternal': false,
	    'options': {
		'resultsAsPoints': true
	    }
	});
	if (skinnerconf instanceof Error) {
		/* XXX need better way to report this */
		throw (skinnerconf);
	}
	this.is_aggregator = stream = skinnerconf.stream;
	stream.on('invalid_object', this.emit.bind(this, 'invalid_object'));
	streams.push(stream);

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

	this.is_dbtmpfilename = dbfilename = DB_TMPFILENAME;
	this.is_dbfilename = DB_FILENAME;
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
	    },
	    'dragnet_index': {}
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

IndexScanner.prototype.sqlite3Escape = function (str)
{
	return (str.replace(/\./g, '_'));
};

IndexScanner.prototype.doIndex = function (chunk, callback)
{
	var row;

	row = new Array(this.is_index.ic_columns.length + 1);

	this.is_index.ic_columns.forEach(function (c, i) {
		mod_assertplus.ok(chunk.fields.hasOwnProperty(c.name));
		row[i] = chunk.fields[c.name];
	});

	row[row.length - 1] = chunk.value;
	this.is_log.debug('indexing', chunk, row);
	this.is_insert.run(row, callback);
};

IndexScanner.prototype.doFlush = function (callback)
{
	var self = this;

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
			st.finalize(function () {
				callback(perr);
			});
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
		stats.ninputs = this.is_skinnerstream.ntransformed;

	stats.aggr_nerr_nonnumeric = 0;
	return (stats);
};
