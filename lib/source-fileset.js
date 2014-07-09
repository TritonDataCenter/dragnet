/*
 * lib/source-fileset.js: data source backed by a set of files structured under
 * a directory tree.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_skinner = require('skinner'); /* XXX */
var mod_stream = require('stream');
var mod_vasync = require('vasync');
var mod_vstream = require('./vstream/vstream');

var mod_dragnet = require('./dragnet'); /* XXX */
var mod_dragnet_impl = require('./dragnet-impl');
var mod_pathenum = require('./path-enum');
var mod_streamutil = require('./stream-util');
var FindStream = require('./fs-find');

var CatStreams = require('catstreams');
var IndexSink = require('./index-sink');
var MultiplexStream = require('./stream-multiplex');
var QueryIndex = require('./query-index');
var StreamScan = require('./stream-scan');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

module.exports = FileSetDataSource;

/*
 * Data source backed by a structured directory tree of files.
 */
function FileSetDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.dataroot, 'args.dataroot');

	this.fss_log = args.log;
	this.fss_dataroot = args.dataroot;
}

/*
 * Given an error, return a stream that will emit that error.  This is used in
 * situations where we want to emit an error from an interface that emits errors
 * asynchronously.
 */
FileSetDataSource.prototype.asyncError = function (err)
{
	var rv = new mod_stream.PassThrough();
	setImmediate(rv.emit.bind(rv, 'error', err));
	return (rv);
};

/*
 * Generate a stream that will emit the names of files that should be scanned
 * for the given query.  These files may be either raw data files or indexes.
 */
FileSetDataSource.prototype.findStream = function (root, query, timeformat)
{
	var finder, pathenum;

	mod_assertplus.string(root, 'root');
	finder = new FindStream({
	    'log': this.fss_log.child({ 'component': 'find' })
	});

	if (query.qc_before === null) {
		finder.write(root);
		finder.end();
		return (finder);
	}

	mod_assertplus.ok(query.qc_after !== null);
	mod_assertplus.string(timeformat, 'timeformat');
	pathenum = mod_pathenum.createPathEnumerator({
	    'pattern': mod_path.join(root, timeformat),
	    'timeStart': query.qc_after,
	    'timeEnd': query.qc_before
	});

	if (pathenum instanceof Error)
		return (pathenum);

	finder.on('error', pathenum.emit.bind(pathenum, 'error'));
	pathenum.pipe(finder);
	return (finder);
};

/*
 * Given a FindStream emitting raw data filenames, attach a pipeline that will
 * read the raw data from disk (as bytes).
 */
FileSetDataSource.prototype.dataStream = function (findstream)
{
	var catstream;

	/*
	 * XXX catstreams should really be writable so that we can pipe this and
	 * get flow control.  For now, we may end up buffering the names of all
	 * files found.
	 */
	catstream = new CatStreams({
	    'log': this.fss_log,
	    'perRequestBuffer': 16834,
	    'maxConcurrency': 2
	});
	mod_vstream.instrumentObject(catstream, { 'name': 'catstream' });
	mod_vstream.instrumentPipelineOps(catstream);

	findstream.on('data', function (fileinfo) {
		if (fileinfo.error) {
			catstream.emit('find_error', fileinfo);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile());
		catstream.cat(function () {
			return (mod_fs.createReadStream(fileinfo.path));
		});
	});

	findstream.on('end', function () {
		catstream.cat(null);
	});

	return (catstream);
};

/*
 * Given a FindStream emitting index filenames, attach a pipeline that will load
 * the indexes, run the query, and emit data points.
 */
FileSetDataSource.prototype.queryStream = function (findstream, query)
{
	var barrier, rv;
	var self = this;

	barrier = mod_vasync.barrier();
	barrier.start('find');

	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 128
	});
	rv.setMaxListeners(Infinity);
	mod_vstream.instrumentObject(rv, { 'name': 'query passthrough' });
	mod_vstream.instrumentTransform(rv, { 'unmarshalIn': false });
	mod_vstream.instrumentPipelineOps(rv);

	/*
	 * XXX Consider making this an object-mode transformer for flow-control.
	 */
	findstream.on('data', function (fileinfo) {
		var queryindex;

		if (fileinfo.error) {
			rv.emit('find_error', fileinfo);
			return;
		}

		mod_assertplus.ok(fileinfo.stat.isFile());
		barrier.start(fileinfo.path);
		queryindex = new QueryIndex({
		    'log': self.fss_log.child({ 'queryindex': fileinfo.path }),
		    'filename': fileinfo.path
		});

		queryindex.on('error', function (err) {
			err = new VError(err, 'index "%s"', fileinfo.path);
			rv.emit('error', err);
		});

		queryindex.on('ready', function () {
			/*
			 * XXX Should this be in a vasync queue to manage
			 * concurrency?
			 */
			var qrun, dnstart, dnts, xform;
			dnstart = queryindex.config().dn_start;
			dnts = parseInt(dnstart, 10);
			if (isNaN(dnts)) {
				rv.emit('error', new VError(
				    'bad "dn_start" in index "%s"',
				    fileinfo.path));
				barrier.done(fileinfo.path);
				return;
			}

			/*
			 * It's a little weird the way this is always tacked-on,
			 * regardless of whether it was requested.  This is
			 * different from the scanning case, when it's tacked on
			 * because the indexing code explicitly adds this field
			 * and defines it in terms of an existing field.
			 */
			qrun = queryindex.run(query);
			xform = mod_streamutil.transformStream({
			    'streamOptions': {
				'objectMode': true,
				'highWaterMark': 0
			    },
			    'func': function (chunk, _, callback) {
				chunk['fields']['__dn_ts'] = dnts;
				this.push(chunk);
				setImmediate(callback);
			    }
			});
			mod_vstream.instrumentObject(xform,
			    { 'name': 'add_dn_ts' });
			mod_vstream.instrumentTransform(xform,
			    { 'unmarshalIn': false });
			mod_vstream.instrumentPipelineOps(xform);

			qrun.pipe(xform);
			xform.pipe(rv, { 'end': false });

			xform.on('end', function () {
				barrier.done(fileinfo.path);
			});
			qrun.on('error', function (err) {
				err = new VError(err, 'index "%s" query',
				    fileinfo.path);
				rv.emit('error', err);
				barrier.done(fileinfo.path);
			});
		});
	});

	findstream.on('end', function () {
		barrier.done('find');
	});

	barrier.on('drain', function () { rv.end(); });
	return (rv);
};

/*
 * "scan" operation arguments:
 *
 *     query		a QueryConfig object
 *
 *     format		string name of file format
 *
 *     [timeFormat]	string describing how data files are organized.
 *     			Must be specified if timeBefore and timeAfter are
 *     			specified with the query.
 *
 * XXX some of this could probably be commonized with the FileDataSource.
 */
FileSetDataSource.prototype.scan = function (args)
{
	var rv = this.scanImpl(args);
	if (rv instanceof Error)
		return (this.asyncError(rv));

	mod_vstream.instrumentTransform(rv,
	    { 'unmarshalIn': false, 'marshalOut': false });
	return (rv);
};

/*
 * Behaves just like scan(), but may return an Error rather than a stream.
 * This exists separately because the real user-facing interface emits
 * validation errors asynchronously, but its useful for the internal interface
 * to be able to distinguish such errors synchronously.
 */
FileSetDataSource.prototype.scanImpl = function (args)
{
	var path, findstream, datastream, parser, scan, rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');

	if (args.query.qc_before !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	/*
	 * The pipeline consists of enumerating directories, finding files in
	 * those directories, concatenating their contents, parsing the results,
	 * and applying the query to that stream of data.  We end with a
	 * pass-through that we use to emit arbitrary other events to our
	 * caller, including asynchronous errors and invalid_* events.
	 */
	path = this.fss_dataroot;
	findstream = this.findStream(path, args.query, args.timeFormat);
	if (findstream instanceof Error)
		return (findstream);

	parser = mod_dragnet_impl.parserFor(args.format);
	if (parser instanceof Error)
		return (parser);

	datastream = this.dataStream(findstream);
	scan = new StreamScan({
	    'query': args.query,
	    'log': this.fss_log.child({ 'component': 'scan' })
	});

	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	});
	mod_vstream.instrumentObject(rv, { 'name': 'scan_op' });
	mod_vstream.instrumentPipelineOps(rv);

	/*
	 * Wire up the pipeline.  The ReadStream can emit an error, but the
	 * others generally won't; instead, they emit "invalid_*" for particular
	 * records that are malformed.  We proxy recoverable errors and these
	 * "invalid_*" events to our caller.
	 */
	findstream.on('error', rv.emit.bind(rv, 'error'));
	datastream.on('error', rv.emit.bind(rv, 'error'));
	scan.on('error', rv.emit.bind(rv, 'error'));

	datastream.pipe(parser);
	parser.pipe(scan);
	scan.pipe(rv);

	/*
	 * Hang a few fields off the last stream for debugging.
	 */
	rv.fss_nfiles = 0;
	rv.fss_dataroot = path;
	rv.stats = function () {
		var st = scan.stats();
		st['nscanned'] = this.fss_nfiles;
		return (st);
	};
	findstream.on('data', function (fileinfo) {
		if (!fileinfo.error)
			rv.fss_nfiles++;
	});

	return (rv);
};

/*
 * "query" operation arguments:
 *
 *    query		a QueryConfig object
 *
 *    indexroot		name of index tree to search
 */
FileSetDataSource.prototype.query = function (args)
{
	var query, path, kinds, haveindexes, rv;
	var self = this;

	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.indexroot, 'args.indexroot');

	query = args.query;
	path = args.indexroot;
	kinds = { 'hour': false, 'day': false };
	haveindexes = 0;
	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 128
	});

	/*
	 * Figure out which indexes are available so that we can use the best
	 * combination of them.
	 */
	mod_vasync.forEachParallel({
	    'inputs': Object.keys(kinds),
	    'func': function (input, callback) {
		var indexpath = mod_path.join(path, 'by_' + input);
		mod_fs.stat(indexpath, function (err, st) {
			/*
			 * There are three results here, and they don't
			 * correlate cleanly with the results of the "stat".
			 *
			 * - ENOENT: the index does not exist.  This is not an
			 *   error unless no indexes exist (which we'll check
			 *   later).
			 *
			 * - Found directory: mark that we have this index.
			 *
			 * - Anything else (a non-ENOENT error or a successful
			 *   stat on a path that turns out not to be a
			 *   directory).  This error is fatal to this operation.
			 *
			 * We only propagate an error to callback() if it's in
			 * the third category.  For the first two, we just
			 * update local state.
			 */
			if (!err && !st.isDirectory())
				err = new VError(
				    'stat "%s": not directory', indexpath);

			if (!err) {
				kinds[input] = true;
				haveindexes++;
				callback();
				return;
			}

			if (err['code'] == 'ENOENT')
				callback();
			else
				callback(err);
		});
	    }
	}, function (err) {
		if (!err && haveindexes === 0) {
			err = new VError('no indexes found');
			rv.emit('error', err);
			return;
		}

		if (err) {
			err = new VError(err, 'load indexes');
			rv.emit('error', err);
			return;
		}

		self.queryExecute({
		    'query': query,
		    'indexroot': path,
		    'indexes': kinds,
		    'stream': rv
		});
	});

	return (rv);
};

/*
 * Plan and execute a query, now that we know which indexes are available.
 */
FileSetDataSource.prototype.queryExecute = function (args)
{
	var rv, parts, upstreams, barrier;
	var self = this;

	rv = args.stream;
	parts = this.queryPlan(args);
	upstreams = parts.map(function (queryargs) {
		return (self.queryImpl(queryargs));
	});

	barrier = mod_vasync.barrier();
	barrier.start('kickoff');
	upstreams.forEach(function (upstream, i) {
		barrier.start(i);
		upstream.pipe(rv, { 'end': false });
		upstream.on('end', function () { barrier.done(i); });
	});

	barrier.on('drain', function () { rv.end(); });
	barrier.done('kickoff');

	rv.stats = function () {
		var st;

		mod_assertplus.ok(upstreams.length > 0);
		st = upstreams[0].stats();
		upstreams.slice(1).forEach(function (u) {
			var st2, k;
			st2 = u.stats();
			for (k in st)
				st[k] += (st2[k] || 0);
		});

		return (st);
	};
};

/*
 * Plan an indexed query: figure out what subqueries to issue to make the best
 * use of the daily and hourly indexes.  In general, higher-level indexes (e.g.,
 * daily) are more efficient than lower-level indexes because there are many
 * fewer of them to scan for a given time range.  There are only a few reasons
 * to prefer a lower-level index over a higher-level one:
 *
 *   o The higher-level index does not support the time granularity required by
 *     the query.  In this case, the lower-level index must be used to execute
 *     the entire query.
 *
 *   o The start or end time of the query implies a resolution more fine-grained
 *     than the higher-level index supports.  For example, the query start time
 *     is 8s before midnight and the higher-level query's resolution is only
 *     10s.  In this case, the lower-level query can be used only for the
 *     endpoint intervals, with the bulk of the query executed based on the
 *     higher-level query.
 *
 *   o The endpoint of the interval is more recent than the most recent
 *     higher-level index.  This might happen in almost-real-time systems, where
 *     daily indexes are produced daily, hourly indexes are produced hourly, and
 *     the query specifies an interval from the current calendar day.
 *
 * We punt on all of these for now.  We always use the highest-level index
 * that's available, which means:
 *
 *    o The results are only meaningfully grouped at a resolution at least as
 *      large as (and aligned with) that index's resolution.  This deals with
 *      the first case above.
 *
 *    o We emit errors for ranges that exceed what's covered by the index.  This
 *      covers the second two cases above.
 *
 * A future implementation could address the first two cases.  The third seems
 * outside the scope of the current implementation.
 */
FileSetDataSource.prototype.queryPlan = function (args)
{
	var query, path, upstreams;

	query = args.query;
	path = args.indexroot;
	upstreams = [];
	if (args.indexes.day) {
		upstreams.push({
		    'query': query,
		    'indexroot': path,
		    'interval': 'day'
		});
	} else {
		upstreams.push({
		    'query': query,
		    'indexroot': path,
		    'interval': 'hour'
		});
	}

	return (upstreams);
};

/*
 * Behaves like query(), but only operates on one set of indexes (e.g., hourly
 * or daily) and may return an Error rather than a stream.  See scanImpl() for
 * why this may return an Error directly.
 */
FileSetDataSource.prototype.queryImpl = function (args)
{
	var path, query, pattern, findstream, querystream, rv, aggr;

	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.string(args.interval, 'args.interval');

	path = mod_path.join(args.indexroot, 'by_' + args.interval);
	query = args.query;

	if (args.interval == 'hour') {
		pattern = '%Y-%m-%d-%H.sqlite';
	} else {
		mod_assertplus.equal(args.interval, 'day');
		pattern = '%Y-%m-%d.sqlite';
	}

	findstream = this.findStream(path, query, pattern);
	if (findstream instanceof Error)
		return (findstream);

	querystream = this.queryStream(findstream, query);

	/*
	 * XXX This is a pretty huge kludge.  The root problem here is that the
	 * indexing operation requires a synthetic __dn_ts field.  For a
	 * scan-based index, it was easy to define the notion of a "synthetic"
	 * field which is the result of parsing the a date field in the raw
	 * data.  The ScanStream implicitly adds a field to its output when
	 * requested.
	 *
	 * For index-based indexes (e.g., an index that's the result of rolling
	 * up another index), we still want a synthetic __dn_ts-like field, and
	 * it's easy to get from the index file itself.  But there's no way for
	 * the indexing code to ask for this property.  If it adds it as a
	 * non-date property, the QueryIndex would try to extract it from the
	 * rows in the database, which isn't right.  If it adds it as a date
	 * property, then the intermediate aggregator inside the QueryIndex
	 * drops all records because it's attempting to aggregate a non-numeric
	 * property (since it's undefined).  If it adds it as a non-numeric
	 * "date" property, then skinner transforms it to a string and we'd have
	 * to parse it again.
	 *
	 * For now, we hardcode the aggregator to always bucketize this
	 * additional property.  When resolving this "XXX", we should remove
	 * this argument, remove support for "extra_bucketizers" from
	 * queryAggrStream(), and remove the "require" of the skinner module
	 * from this file.
	 */
	aggr = mod_dragnet_impl.queryAggrStream({
	    'query': query,
	    'options': { 'resultsAsPoints': true },

	    'extra_bucketizers': [ {
	        'name': '__dn_ts',
		'bucketizer': mod_skinner.makeLinearBucketizer(1)
	    } ]
	});
	rv = new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	});

	/*
	 * See scanImpl().
	 */
	findstream.on('error', rv.emit.bind(rv, 'error'));
	querystream.on('error', rv.emit.bind(rv, 'error'));
	querystream.on('find_error', rv.emit.bind(rv, 'find_error'));
	aggr.on('error', rv.emit.bind(rv, 'error'));

	querystream.pipe(aggr);
	aggr.pipe(rv);

	rv.fsq_nfiles = 0;
	rv.stats = function () {
		var st = aggr.stats();
		st['nscanned'] = this.fsq_nfiles;
		return (st);
	};
	findstream.on('data', function (fileinfo) {
		if (!fileinfo.error)
			rv.fsq_nfiles++;
	});

	return (rv);
};


/*
 * "index" operation arguments:
 *
 *     index		an IndexConfig object
 *
 *     indexroot	string path to where indexes should go
 *
 *     format		string name of file format
 *
 *     [source]		'raw' (default) means to scan raw data;
 *     			'hour' means to query hourly indexes, and so on.
 *
 *     [timeFormat]	see scan().
 */
FileSetDataSource.prototype.index = function (args)
{
	var index, query, scan, multiplexer, root;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.string(args.source, 'args.source');

	if (args.index.ic_before !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	index = args.index;
	root = mod_path.join(args.indexroot, 'by_' + index.ic_interval);

	if (args.source == 'raw') {
		if (args.index.ic_timefield === null)
			return (this.asyncError(new VError(
			    'at least one field must be a "date"')));
		query = mod_dragnet_impl.indexQuery(index);
	} else {
		query = mod_dragnet_impl.indexQuery(index, true);
	}

	/*
	 * Indexes can operate on either raw data or a lower-level index (e.g.,
	 * daily indexes can be generated from hourly indexes).  Both the
	 * scan-based and query-based streams generated aggregate data points in
	 * the same format.
	 */
	if (args.source == 'raw') {
		scan = this.scanImpl({
		    'query': query,
		    'format': args.format,
		    'timeFormat': args.timeFormat
		});
	} else {
		scan = this.queryImpl({
		    'query': query,
		    'indexroot': args.indexroot,
		    'interval': args.source
		});
	}

	if (scan instanceof Error)
		return (this.asyncError(scan));

	/*
	 * When generating an "hourly" or "daily" Dragnet index, we really
	 * generate a group of per-hour or per-day index files.  Each of these
	 * files is generated by an IndexSink stream, which accepts aggregated
	 * data points (as emitted by either the scanner or querier) and writes
	 * them into a sqlite index file.
	 *
	 * The aggregated data points may be emitted in any order.  A
	 * Multiplexer is responsible for looking at the timestamp in each data
	 * point and deciding which IndexSink stream that point should be
	 * written to.  IndexSinks (and the corresponding index files) are
	 * created only as needed.
	 */
	multiplexer = new MultiplexStream({
	    'log': self.fss_log.child({ 'component': 'multiplexer' }),

	    'streamOptions': { 'highWaterMark': 0 },

	    'bucketer': function (record) {
		var dnts, tsms, tsdate, datestr, bucketname, bucketstart;

		dnts = record['fields']['__dn_ts'];
		mod_assertplus.equal(typeof (dnts), 'number');
		mod_assertplus.ok(!isNaN(dnts));
		tsms = dnts * 1000;
		tsdate = new Date(tsms);
		datestr = tsdate.toISOString();

		if (index.ic_interval == 'hour') {
			bucketname = datestr.substr(0, '2014-07-02T00'.length);
			bucketstart = Date.parse(bucketname + ':00:00Z') / 1000;
		} else {
			mod_assertplus.equal(index.ic_interval, 'day');
			bucketname = datestr.substr(0, '2014-07-02'.length);
			bucketstart = Date.parse(bucketname + 'T00:00:00Z') /
			    1000;
		}

		return ({
		    'name': bucketname,
		    'timestamp': tsdate,
		    'start': bucketstart
		});
	    },

	    'bucketCreate': function (_, bucketdesc) {
		var label, indexpath, indexsink;

		label = bucketdesc.name.replace(/T/, '-');
		indexpath = mod_path.join(root, label + '.sqlite');
		indexsink = new IndexSink({
		    'log': self.fss_log.child({ 'indexer': label }),
		    'index': index,
		    'filename': indexpath,
		    'config': {
			'dn_start': bucketdesc.start
		    }
		});
		indexsink.on('error', function (err) {
			multiplexer.emit('error',
			    new VError(err, 'index "%s"', label));
		});
		return (indexsink);
	    }
	});

	scan.pipe(multiplexer);
	scan.on('error',
	    multiplexer.emit.bind(multiplexer, 'error'));
	scan.on('find_error',
	    multiplexer.emit.bind(multiplexer, 'find_error'));
	scan.on('invalid_object',
	    multiplexer.emit.bind(multiplexer, 'invalid_object'));
	scan.on('invalid_record',
	    multiplexer.emit.bind(multiplexer, 'invalid_record'));

	/*
	 * XXX this is the wrong stream to return in that when the caller gets
	 * "finish", that doesn't mean we've flushed the indexers.
	 */
	multiplexer.stats = function () { return (scan.stats()); };
	return (multiplexer);
};
