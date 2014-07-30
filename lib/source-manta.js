/*
 * lib/source-manta.js: Manta-based Dragnet backend.
 */

var mod_assertplus = require('assert-plus');
var mod_fs = require('fs');
var mod_fstream = require('fstream');
var mod_jsprim = require('jsprim');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_tar = require('tar');
var mod_vasync = require('vasync');
var mod_zlib = require('zlib');
var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

var MantaFinder = require('./manta-find');
var mod_dragnet_impl = require('./dragnet-impl');
var mod_streamutil = require('./stream-util');
var mod_timeutil = require('./time-util');
var mod_vstream = require('./vstream/vstream');

module.exports = MantaDataSource;

var JOBTPL_INIT_UNPACK_ASSET = 'tar xzf "/assets%s"';
var JOBTPL_EXEC_SCAN0 = './dragnet/bin/dn scan-file --points --data-format=%s';
var JOBTPL_EXEC_QUERY0 = './dragnet/bin/dn query-file --points %s ' +
    '$MANTA_INPUT_FILE';
/*
 * It would be nice to support hourly indexes with "index-file /dev/stdin"
 * rather than having to create a temporary tree for "index-tree".
 */
var JOBTPL_EXEC_INDEX = 'set -o errexit; ' +
    'set -o pipefail; ' +
    'rm -rf /index /var/tmp/dn; ' +
    'mkdir /var/tmp/dn; ' +
    'cat > /var/tmp/dn/data; ' +
    'node --max-old-space-size=1900 ' +
    '--abort-on-uncaught-exception ' +
    './dragnet/bin/dn ' +
    'index-tree --data-format=%s %s %s /var/tmp/dn /index; ' +
    'if [[ -d /index ]]; then ' +
    'cd /index; ' +
    'tar cf /index.tar . ; ' +
    'muntar -f /index.tar "%s"; '+
    'fi';
var JOBTPL_EXEC_MSPLIT = '| msplit -n %d -j ' +
    '-e \'Math.floor(this.fields.timestamp / %d).toString()\'';
var FINDFILTER_TRUE = function () { return (true); };
var DRAGNET_VERSION;

/*
 * Given a query, return an array of "dn" command-line arguments that represent
 * the same query.  These are already quoted and escaped as necessary -- they
 * could be joined with whitespace and safely appended directly to a command
 * line.
 */
function queryToCliArgs(query, subsequent, fieldsarg)
{
	var rv, str, fields;

	rv = [];
	if (!subsequent) {
		if (query.qc_filter !== null) {
			str = JSON.stringify(query.qc_filter);
			rv.push(sprintf('--filter \'%s\'', str));
		}

		if (query.qc_before !== null) {
			mod_assertplus.ok(query.qc_after !== null);
			str = query.qc_before.toISOString();
			rv.push(sprintf('--before "%s"', str));
			str = query.qc_after.toISOString();
			rv.push(sprintf('--after "%s"', str));
		}

		/*
		 * If there was an extra time field, we need to pass that
		 * through to the first phase.
		 */
		fields = query.qc_synthetic.filter(function (fc) {
			return (!query.qc_fieldsbyname.hasOwnProperty(fc.name));
		});
		if (fields.length > 0) {
			mod_assertplus.ok(fields.length == 1);
			rv.push(sprintf('--time-field=%s', fields[0].name));
		}
	}

	fields = [];
	query.qc_breakdowns.forEach(function (ofc) {
		var fc, keys, hasextra, values;

		/*
		 * The first phase is a direct translation of the command-line
		 * arguments, but subsequent phases are special in the case of
		 * synthetic fields, since these values will already be present
		 * in those phases, and the origin field likely won't.  We
		 * rewrite these here, and we assert that such fields (which are
		 * always dates) are aggregated in order to make sure the
		 * numeric type is preserved.  This is pretty gnarly.
		 */
		if (subsequent) {
			fc = mod_jsprim.deepCopy(ofc);
			if (fc.hasOwnProperty('date') &&
			    fc.field != fc.name) {
				mod_assertplus.ok(fc.hasOwnProperty('aggr'),
				    'synthetic "date" field must be ' +
				    'aggregated');
				fc.field = fc.name;
			}
		} else {
			fc = ofc;
		}

		keys = Object.keys(fc);
		hasextra = fc.name != fc.field;
		values = [];

		keys.forEach(function (key) {
			if (key != 'name') {
				if (!fc[key])
					values.push(shEscape(key));
				else
					values.push(sprintf('%s=%s',
					    shEscape(key),
					    shEscape(fc[key].toString())));
			}

			if (key == 'date' || key == 'aggr') {
				hasextra = true;
				return;
			}

			if (key == 'step' || key == 'name' || key == 'field')
				return;

			/*
			 * XXX Blow up here if we encounter a field
			 * configuration that we don't know how to serialize.
			 * It would be nice if we could verify this statically,
			 * or at least if the field configuration object ("fc")
			 * had a more rigid set of fields with prefixed names so
			 * that if someone adds a new field, they could
			 * reasonably be asked to look for references like this
			 * one.
			 */
			throw (new VError('internal error: don\'t know how ' +
			    'serialize field property "%s"', key));
		});

		if (!hasextra) {
			fields.push(shEscape(fc.name));
			return;
		}

		fields.push(sprintf('%s[%s]', shEscape(fc.name),
		    values.join('\\;')));
	});

	if (fields.length > 0)
		/* XXX escape commas, semicolons, and spaces in field names? */
		rv.push(sprintf('--%s=%s', fieldsarg, fields.join(',')));

	return (rv);
}

/*
 * Given an index, return an array of "dn" command-line arguments that represent
 * the same index configuration, not including the query-related arguments.
 */
function indexToCliArgs(index)
{
	return ([ sprintf('--interval=%s', index.ic_interval) ]);
}

/*
 * This function should logically escape characters that would be wrongly
 * interpreted by the shell, but in practice we're just going to validate that
 * they're not present and escape the few that are simple to handle.
 * XXX The assertions in this functions are programming errors.  These should be
 * replaced with proper support or error handling.
 */
function shEscape(str)
{
	/*
	 * Only allow what we know is either safe or we can safely escape:
	 * alphanumerics, ".", "-", "_", " ", and "$".
	 */
	if (typeof (str) == 'number')
		str = str.toString();
	if (/[^/a-zA-Z0-9 $|_\-\.]/.test(str))
		throw (new VError('unsupported token: "%s"', str));
	return (str.replace(/([ $|])/g,
	    function (_, $1) { return ('\\' + $1); }));
}

/*
 * Arguments:
 *
 *     log		a bunyan-style logger
 *
 *     manta		a node-manta client
 *
 *     [dataroot]	root of data stored in Manta
 *     			(required for subsequent scan operations)
 */
function MantaDataSource(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.optionalString(args.dataroot, 'args.dataroot');

	this.mds_log = args.log;
	this.mds_manta = args.manta;
	this.mds_dataroot = args.dataroot;

	/*
	 * This is a little janky, but it's useful to know our version number
	 * (which we use for our asset name) in synchronous contexts and we
	 * don't want to block in those contexts.
	 */
	if (DRAGNET_VERSION === undefined) {
		var filename, contents, pkginfo;
		filename = mod_path.normalize(mod_path.join(
		    __dirname, '..', 'package.json'));
		try {
			contents = mod_fs.readFileSync(filename);
			pkginfo = JSON.parse(contents);
		} catch (ex) {
			throw (new VError(ex, 'failed to determine Dragnet ' +
			    'package version from "%s"'));
		}

		DRAGNET_VERSION = pkginfo['version'];
	}
}

/*
 * See FileSetDataSource.asyncError().
 */
MantaDataSource.prototype.asyncError = function (err)
{
	var rv = new mod_stream.PassThrough();
	setImmediate(rv.emit.bind(rv, 'error', err));
	return (rv);
};

/*
 * Arguments:
 *
 *     jobName			name of job
 *
 *     query			QueryConfig object
 *
 *     format			raw input data format
 *
 *     [timeFormat]		format string for describing directory tree
 *     				structure
 *
 *     nextrareducers		number of extra, pre-aggregating reducer phases
 *     				to apply
 *
 *     nreducercount		number of reducers at each extra reduce phase
 *
 *     [msplitcount]		if specified, the last phase should "msplit" the
 *     				output to "msplitcount" reducers
 *
 *     [msplitinterval]		if specified, the last phase msplit should group
 *     				records into time chunks of size
 *     				"msplitinterval" seconds (e.g., 3600 for an
 *     				hour)
 */
MantaDataSource.prototype.scanJobInit = function (args)
{
	var query, error, job, inputstream, i, mpipe;

	mod_assertplus.string(args.jobName, 'args.jobName');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.number(args.nextrareducers, 'args.nextrareducers');
	mod_assertplus.number(args.nreducercount, 'args.nreducercount');
	mod_assertplus.string(this.mds_dataroot);

	mod_assertplus.optionalNumber(args.msplitcount, 'args.msplitcount');
	if (args.msplitcount)
		mod_assertplus.optionalNumber(args.msplitinterval,
		    'args.msplitinterval');

	/*
	 * The caller's caller is responsible for making sure that timeFormat is
	 * supplied if before/after are supplied, so if we make it this far with
	 * the wrong arguments, that's a programming error.
	 */
	query = args.query;
	if (query.qc_before !== null && query.qc_after !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	/*
	 * Validate the format name because we're going to be inserting it into
	 * a bash script directly.
	 */
	mod_assertplus.string(args.format, 'args.format');
	error = mod_dragnet_impl.parserFor(args.format);
	if (error instanceof Error)
		return (error);

	/*
	 * List inputs by scanning the Manta directory tree under "root" using
	 * the given time format, with the before/after constraints implied by
	 * the query.
	 */
	inputstream = this.listInputs(
	    this.mds_dataroot, args.timeFormat, query);
	if (inputstream instanceof Error)
		return (inputstream);

	/*
	 * Build up the job.  All scan-related jobs do a scan in the first
	 * phase, translating the query parameters into command-line arguments
	 * for a "dn scan-file" invocation.  They all use the same dragnet asset
	 * as well.
	 */
	mpipe = '';
	if (args.msplitcount && args.msplitcount > 1 &&
	    args.nextrareducers === 0)
		mpipe = sprintf(JOBTPL_EXEC_MSPLIT, args.msplitcount,
		    args.msplitinterval);

	job = this.jobBuilder();
	job.inputstream = inputstream;
	job.builder.name(args.jobName);
	job.builder.phase({
	    'type': 'map',
	    'assets': [ job.assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, args.format) + ' ' +
	        queryToCliArgs(query, false, 'breakdowns').join(' ') +
		' /dev/stdin' + mpipe
	});

	/*
	 * We can apply however many reducers are desired to pre-aggregate
	 * before the final, single reducer (which is caller-specific).
	 */
	for (i = 0; i < args.nextrareducers; i++) {
		if (args.msplitcount && args.msplitcount > 1 &&
		    i == args.nextrareducers - 1)
			mpipe = sprintf(JOBTPL_EXEC_MSPLIT, args.msplitcount,
			    args.msplitinterval);
		job.builder.phase({
		    'type': 'reduce',
		    'memory': 4096,
		    'count': args.nreducercount,
		    'assets': [ job.assetpath ],
		    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
		    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner') + ' ' +
			queryToCliArgs(query, true, 'breakdowns').join(' ') +
			' /dev/stdin' + mpipe
		});
	}

	return (job);
};

/*
 * "scan" arguments:
 *
 *     query		a QueryConfig object
 *
 *     format		string name of the file format
 *
 *     dryRun		if true, don't actually run the job
 *
 *     timeFormat	specifies how paths are constructed based on dates.
 *     			Required for --before or --after.
 *
 *     extraReduceCount	number of reducers per extra reduce phase
 *
 *     extraReducePhases	number of extra reducer phases
 */
MantaDataSource.prototype.scan = function (args)
{
	var query, job, rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.number(args.extraReduceCount, 'args.extraReduceCount');
	mod_assertplus.number(args.extraReducePhases, 'args.extraReducePhases');
	mod_assertplus.string(this.mds_dataroot);

	query = args.query;
	job = this.scanJobInit({
	    'jobName': 'dragnet scan',
	    'query': query,
	    'format': args.format,
	    'timeFormat': args.timeFormat,
	    'nextrareducers': args.extraReducePhases,
	    'nreducercount': args.extraReduceCount
	});
	if (job instanceof Error)
		return (this.asyncError(job));

	job.builder.phase({
	    'type': 'reduce',
	    'memory': 4096,
	    'assets': [ job.assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner') + ' ' +
		queryToCliArgs(query, true, 'breakdowns').join(' ') +
		' /dev/stdin'
	});

	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	}));

	if (args.dryRun)
		this.jobDryRun(job, function () { rv.end(); });
	else
		this.jobRunToEnd(job, rv, false);

	return (rv);
};

MantaDataSource.prototype.index = function (args)
{
	var query, job, rv;
	var msplitcount = 1;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.index, 'args.index');
	mod_assertplus.string(args.indexroot, 'args.indexroot');
	mod_assertplus.string(args.format, 'args.format');
	mod_assertplus.number(args.extraReduceCount, 'args.extraReduceCount');
	mod_assertplus.number(args.extraReducePhases, 'args.extraReducePhases');

	if (args.index.ic_timefield === null)
		return (this.asyncError(new VError(
		    'at least one field must be a "date"')));

	query = mod_dragnet_impl.indexQuery(args.index, true);
	job = this.scanJobInit({
	    'jobName': 'dragnet index',
	    'query': query,
	    'format': args.format,
	    'timeFormat': args.timeFormat,
	    'nextrareducers': args.extraReducePhases,
	    'nreducercount': args.extraReduceCount,
	    'msplitcount': msplitcount,
	    'msplitinterval': args.index.ic_interval == 'hour' ?
	        3600 : 86400
	});
	if (job instanceof Error)
		return (this.asyncError(job));

	job.builder.phase({
	    'type': 'reduce',
	    'memory': 4096,
	    'count': msplitcount,
	    'assets': [ job.assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
	    'exec': sprintf(JOBTPL_EXEC_INDEX, 'json-skinner',
	        queryToCliArgs(query, true, 'columns').join(' '),
		indexToCliArgs(args.index).join(' '), shEscape(args.indexroot))
	});

	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough());
	rv.on('end', function () { rv.emit('flushed'); });
	rv.read(0);

	if (args.dryRun)
		this.jobDryRun(job, function () { rv.end(); });
	else
		this.jobRunToEnd(job, rv, true);

	return (rv);
};

MantaDataSource.prototype.jobBuilder = function ()
{
	var assetpath, builder;

	mod_assertplus.ok(process.env['MANTA_USER']);
	assetpath = sprintf('/%s/stor/dragnet-%s.tgz',
	    process.env['MANTA_USER'], DRAGNET_VERSION);
	builder = new JobBuilder();
	return ({
	    'assetpath': assetpath,
	    'builder': builder
	});
};

MantaDataSource.prototype.query = function (args)
{
	var query, job, inputstream, rv;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.indexroot, 'args.indexroot');

	/* XXX figure out hourly vs. daily */
	query = args.query;
	inputstream = this.listInputs(args.indexroot,
	    'by_day/%Y-%m-%d.sqlite', query);
	if (inputstream instanceof Error)
		return (this.asyncError(inputstream));

	job = this.jobBuilder();
	job.inputstream = inputstream;
	job.builder.name('dragnet query');
	job.builder.phase({
	    'type': 'map',
	    'assets': [ job.assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
	    'exec': sprintf(JOBTPL_EXEC_QUERY0,
	        queryToCliArgs(query, false, 'breakdowns').join(' '))
	});

	job.builder.phase({
	    'type': 'reduce',
	    'assets': [ job.assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, job.assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner') + ' ' +
		queryToCliArgs(query, true, 'breakdowns').join(' ') +
		' /dev/stdin'
	});

	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	}));

	if (args.dryRun)
		this.jobDryRun(job, function () { rv.end(); });
	else
		this.jobRunToEnd(job, rv, false);

	return (rv);
};

/*
 * Execute a job dry run, which just means printing out the job definition and
 * inputs.
 */
MantaDataSource.prototype.jobDryRun = function (job, callback)
{
	console.error(job.builder.json(true));
	console.error('\nInputs:');
	job.inputstream.on('data', function (chunk) {
		console.error(chunk.path);
	});
	job.inputstream.on('end', callback);
};

/*
 * Generate the list of inputs for a job.  Given a Manta path ("root"), a time
 * format string ("timeformat"), and a query that may contain before/after
 * constraints, return a MantaFinder stream that emits objects for each Manta
 * object that should be searched.
 */
MantaDataSource.prototype.listInputs = function (root, timeformat, query)
{
	var filter, finder;

	filter = this.findFilterFunction(root, timeformat, query);
	if (filter instanceof Error)
		return (filter);

	finder = new MantaFinder({
	    'log': this.mds_log.child({ 'component': 'find' }),
	    'manta': this.mds_manta,
	    'root': root,
	    'filter': filter
	});

	return (finder);
};

/*
 * See listInputs().  This helper returns a filter function suitable for a
 * MantaFinder that will prune out directories that should not be searched to
 * find inputs.
 */
MantaDataSource.prototype.findFilterFunction = function (root, formatstr, query)
{
	var filter;

	if (query.qc_before === null && query.qc_after === null)
		return (FINDFILTER_TRUE);

	filter = mod_timeutil.createTimeStringFilter(formatstr);
	if (filter instanceof Error)
		return (filter);

	return (function filterPathByTime(path) {
		var tail;
		mod_assertplus.ok(mod_jsprim.startsWith(path, root));
		tail = path.substr(root.length);
		return (filter.rangeContains(query.qc_after,
		    query.qc_before, tail));
	});
};

/*
 * Submit a Manta job, wait for it to complete, and dump the contents of the
 * outputs to "rv".  This function makes sure that the dragnet asset is present
 * at "assetpath" before submitting the job.
 */
MantaDataSource.prototype.jobRunToEnd = function (job, rv, skipoutput)
{
	var builder, inputstream, assetpath;
	var log, manta, barrier, inputs;

	builder = job.builder;
	inputstream = job.inputstream;
	assetpath = job.assetpath;
	log = this.mds_log;
	manta = this.mds_manta;
	barrier = mod_vasync.barrier();
	inputs = [];

	barrier.start('collect inputs');
	barrier.start('submit job');
	inputstream.on('data', function (chunk) { inputs.push(chunk.path); });
	inputstream.on('end', function () { barrier.done('collect inputs'); });

	mod_vasync.waterfall([
	    function jobCheckAsset(callback) {
		log.trace('checking for "%s"', assetpath);
		manta.info(assetpath, function (err) {
			if (err) {
				if (err['code'] == 'NotFoundError') {
					callback(null, false);
				} else {
					callback(new VError(err,
					    'checking asset'));
				}
			} else {
				callback(null, true);
			}
		});
	    },
	    function jobMakeAsset(skipupload, callback) {
		var dir, fstream, tarpacker, zipper;

		if (skipupload) {
			console.error('using existing asset: "%s"', assetpath);
			callback();
			return;
		}

		if (process.platform != 'sunos') {
			callback(new VError('Required asset not found at ' +
			    '"%s".  Run on SmartOS (e.g., in mlogin(1) ' +
			    'session) to automatically create the asset.',
			    assetpath));
			return;
		}

		dir = mod_path.normalize(mod_path.join(__dirname, '..'));
		console.error('uploading tarball of "%s" to "%s"',
		    dir, assetpath);
		log.debug('uploading asset');
		fstream = mod_fstream.Reader({
		    'path': dir,
		    'type': 'Directory'
		});
		tarpacker = mod_tar.Pack({ 'noProprietary': true });
		zipper = mod_zlib.createGzip();

		fstream.pipe(tarpacker);
		tarpacker.pipe(zipper);
		manta.put(assetpath, zipper, function (err) {
			if (err)
				callback(new VError(err, 'upload asset'));
			else
				callback();
		});
	    },
	    function jobCreate(callback) {
		var jobdef = builder.job();
		log.debug('submitting job', jobdef);
		manta.createJob(jobdef, function (err, jobid) {
			if (err) {
				callback(new VError(err, 'submit job'));
			} else {
				console.error('submitted job %s', jobid);
				callback(null, jobid);
			}
		});
	    },
	    function waitCollectInputs(jobid, callback) {
		log.debug('waiting to collect inputs');
		barrier.on('drain', function () { callback(null, jobid); });
		barrier.done('submit job');
	    },
	    function jobSubmitInputs(jobid, callback) {
		log.debug('submitting inputs and endinput input');
		manta.addJobKey(jobid, inputs, { 'end': true }, function (err) {
			if (err) {
				callback(new VError(err, 'add inputs'));
			} else {
				console.error('submitted %d inputs',
				    inputs.length);
				callback(null, jobid);
			}
		});
	    },
	    function jobWait(jobid, callback) {
		/* XXX parameterize text logging stream */
		log.debug('waiting for job', jobid);
		setTimeout(function () {
			log.debug('checking on job', jobid);
			manta.job(jobid, function (err, state) {
				log.trace('job state', err, state);
				if (err)
					callback(new VError(err, 'fetch job'));
				else if (state['state'] == 'done')
					callback(null, state);
				else
					jobWait(jobid, callback);
			});
		}, 1000);
	    },
	    function jobFetchOutputList(state, callback) {
		if (state.stats.errors > 0) {
			callback(new VError('job "%s" had %d errors',
			    state.id, state.stats.errors));
			return;
		}

		if (skipoutput) {
			callback(null, null);
			return;
		}

		if (state.stats.outputs != 1) {
			callback(new VError('job "%s" unexpectedly had' +
			    '%d outputs', state.stats.outputs));
			return;
		}

		manta.jobOutput(state.id, function (err, emitter) {
			if (err) {
				callback(new VError(err, 'fetch outputs'));
				return;
			}

			var objnames = [];
			emitter.on('key', function (k) { objnames.push(k); });
			emitter.on('end', function () {
				if (objnames.length != 1) {
					callback(new VError('server ' +
					    'unexpectedly returned %d outputs',
					    objnames.length));
					return;
				}

				callback(null, objnames[0]);
			});
		});
	    },
	    function jobFetchOutputObject(path, callback) {
		if (skipoutput) {
			setImmediate(function () { rv.end(); });
			callback();
			return;
		}

		manta.get(path, function (err, stream) {
			if (err) {
				callback(new VError(err, 'fetch "%s"', path));
				return;
			}

			var parser = mod_dragnet_impl.parserFor('json-skinner');
			stream.pipe(parser);
			parser.pipe(rv);
			callback();
		});
	    }
	], function (err) {
		if (err)
			rv.emit('error', err);
	});
};

/*
 * Helper class for constructing a Manta job definition.
 */
function JobBuilder()
{
	this.jb_name = '';
	this.jb_phases = [];
}

/*
 * Set the name of the job.
 */
JobBuilder.prototype.name = function (name)
{
	mod_assertplus.string(name, 'name');
	this.jb_name = name;
};

/*
 * Add a phase to the job.
 */
JobBuilder.prototype.phase = function (phase)
{
	mod_assertplus.object(phase, 'phase');
	mod_assertplus.string(phase.type, 'phase.type');
	mod_assertplus.optionalArrayOfString(phase.assets, 'phase.assets');
	mod_assertplus.optionalString(phase.init, 'phase.init');
	mod_assertplus.string(phase.exec, 'phase.exec');
	this.jb_phases.push(phase);
};

/*
 * Return the job definition, as a JS object.
 */
JobBuilder.prototype.job = function ()
{
	return ({
	    'name': this.jb_name,
	    'phases': this.jb_phases
	});
};

/*
 * Return the job definition as JSON text.
 */
JobBuilder.prototype.json = function (pretty)
{
	return (JSON.stringify(this.job(), null, pretty ? '    ' : ''));
};
