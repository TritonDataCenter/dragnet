/*
 * lib/datasource-manta.js: implementation of Datasource for Manta-based data
 * sources
 */

var mod_assertplus = require('assert-plus');
var mod_manta = require('manta');
var mod_fs = require('fs');
var mod_fstream = require('fstream');
var mod_jsprim = require('jsprim');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_tar = require('tar');
var mod_vasync = require('vasync');
var mod_vstream = require('vstream');
var mod_zlib = require('zlib');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

var MantaFinder = require('./manta-find');
var MantaJobBuilder = require('./manta-job-builder.js');
var mod_dragnet_impl = require('./dragnet-impl');
var mod_streamutil = require('./stream-util');
var mod_timeutil = require('./time-util');

/* Public interface */
exports.createDatasource = createDatasource;

var FINDFILTER_TRUE = function () { return (true); };
var DRAGNET_VERSION;

/*
 * Scripts that run inside jobs.
 */
var JOBTPL_INIT_UNPACK_ASSET = 'tar xzf "/assets%s"';
var JOBTPL_EXEC_SCAN0 = [
    'export DRAGNET_CONFIG=/dragnetrc',
    'export PATH=$PATH:/dragnet/bin',
    'set -o pipefail',
    'set -o errexit',
    'rm -f $DRAGNET_CONFIG',
    'dn datasource-add input --path=/dev/stdin --data-format=%s %s',
    'dn scan --points input'
].join('\n');
var JOBTPL_EXEC_INDEX0 = [
    'export DRAGNET_CONFIG=/dragnetrc',
    'export PATH=$PATH:/dragnet/bin',
    'set -o pipefail',
    'set -o errexit',
    'rm -f $DRAGNET_CONFIG',
    'dn datasource-add input --path=/dev/stdin --data-format=%s %s',
    'dn index-scan --index-config=/assets%s --interval=%s %s input'
].join('\n');
var JOBTPL_EXEC_QUERY0 = [
    'export DRAGNET_CONFIG=/dragnetrc',
    'export PATH=$PATH:/dragnet/bin',
    'set -o pipefail',
    'set -o errexit',
    'rm -f $DRAGNET_CONFIG',
    'dn datasource-add input --path=/dev/null --index-path=/manta%s',
    'dn query --points input'
].join('\n');
var JOBTPL_EXEC_INDEX_REDUCE = [
    'export DRAGNET_CONFIG=/dragnetrc',
    'export PATH=$PATH:/dragnet/bin',
    'set -o pipefail',
    'set -o errexit',
    'rm -f $DRAGNET_CONFIG',
    'rm -rf /index',
    /* This path is /dev/null to emphasize that it's never read. */
    'dn datasource-add input --path=/dev/null --index-path=/index',
    'dn index-read --index-config=/assets%s --interval=%s input', /* stdin */
    'if [[ -d /index ]]; then ',
    '    cd /index',
    '    tar cf /index.tar .',
    '    muntar -f /index.tar "%s"',
    'fi'
].join('\n');

function createDatasource(args)
{
	var dsconfig;

	mod_assertplus.object(args);
	mod_assertplus.object(args.dsconfig);
	mod_assertplus.object(args.log);

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
			return (new VError(ex, 'failed to determine Dragnet ' +
			    'package version from "%s"'));
		}

		DRAGNET_VERSION = pkginfo['version'];
	}

	dsconfig = args.dsconfig;
	mod_assertplus.equal(dsconfig.ds_backend, 'manta');
	if (typeof (dsconfig.ds_backend_config.path) != 'string')
		return (new VError('expected datasource "path" ' +
		    'to be a string'));
	return (new DatasourceManta(args));
}

function DatasourceManta(args)
{
	this.ds_format = args.dsconfig.ds_format;
	this.ds_timeformat = args.dsconfig.ds_backend_config.timeFormat || null;
	this.ds_timefield = args.dsconfig.ds_backend_config.timeField || null;
	this.ds_datapath = args.dsconfig.ds_backend_config.path;
	this.ds_indexpath = args.dsconfig.ds_backend_config.indexPath || null;
	this.ds_filter = args.dsconfig.ds_filter || null;

	this.ds_log = args.log;
	this.ds_manta = mod_manta.createBinClient({
	    'log': args.log.child({ 'component': 'manta' })
	});

	/* Bad, manta client! */
	process.removeAllListeners('uncaughtException');
}

/*
 * [public] Clean up any resources opened by this datasource.
 */
DatasourceManta.prototype.close = function ()
{
	this.ds_manta.close();
};

/*
 * [public] Scan raw data to execute a query.  Arguments:
 *
 *     query		describes the query the user wants to execute
 *
 *     dryRun		if true, just print what would be done
 *
 *     assetroot	Manta path where Dragnet assets are stored
 */
DatasourceManta.prototype.scan = function (args)
{
	var error, query, inputstream, assetpath, job, rv, jobinfo;
	var timefield;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.assetroot, 'args.assetroot');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');

	/*
	 * Validate that we have the necessary configuration to process
	 * time-based constraints.
	 */
	error = null;
	query = args.query;
	if (query.qc_before !== null || query.qc_after !== null) {
		if (this.ds_timeformat === null) {
			error = new VError('datasource missing "timeformat" ' +
			    'property for --before/--after');
		} else if (this.ds_timefield === null) {
			error = new VError('datasource missing "timefield" ' +
			    'property for --before/--after');
		} else {
			timefield = sprintf(' --time-field=%s',
			    this.ds_timefield);
		}
	} else {
		timefield = '';
	}

	/*
	 * Validate the format name, even though we won't use the parser.
	 */
	if (error === null) {
		error = mod_dragnet_impl.parserFor(this.ds_format);
		if (!(error instanceof Error))
			error = null;
	}

	if (error === null) {
		inputstream = this.listInputs(this.ds_datapath,
		    this.ds_timeformat, query.qc_after, query.qc_before);
		if (inputstream instanceof Error)
			error = inputstream;
	}

	if (error !== null)
		return (mod_dragnet_impl.asyncError(error));

	assetpath = this.assetpath(args.assetroot);
	job = new MantaJobBuilder();
	job.name('dragnet scan');
	job.phase({
	    'type': 'map',
	    'assets': [ assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, this.ds_format, timefield) +
	        ' ' + this.queryToCliArgs(query, false, true).join(' ')
	});

	job.phase({
	    'type': 'reduce',
	    'memory': 2048,
	    'assets': [ assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner', '') + ' ' +
		this.queryToCliArgs(query, true, false).join(' ')
	});

	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	}));

	jobinfo = {
	    'assetpath': assetpath,
	    'builder': job,
	    'inputstream': inputstream
	};

	if (args.dryRun)
		this.jobDryRun(jobinfo, function () { rv.end(); });
	else
		this.jobRunToEnd(jobinfo, rv, function () {});

	return (rv);
};

/*
 * [public] Build an index to support the given metrics.  Arguments:
 *
 *     metrics		Non-empty array of metrics that should be supported, as
 *     			metric configuration objects.
 *
 *     dryRun		If true, just print out what would be done.
 *
 *     interval		How to chunk up indexes (e.g., 'hour', 'day').
 *
 *     assetroot	Path to Dragnet assets in Manta
 *
 *     [timeAfter]	If specified, only scan data after this timestamp.
 *
 *     [timeBefore]	If specified, only scan data before this timestamp.
 *
 * With each index we store an index configuration file that describes the
 * metrics provided by the index.  For now, whenever we rebuild the index, we
 * clobber this metadata.
 *
 * It would be nice to record information about which parts of the index have
 * been built for which metrics.  The simplest case for this is just keeping
 * track of the last time the index was built so that users can just type
 * "build" and have Dragnet figure out what's missing and build it.
 */
DatasourceManta.prototype.build = function (args, callback)
{
	var self = this;
	var error, manta, log, idxconfig, idxconfigpath;
	var inputstream, assetpath, job, timefield, extraargs;

	error = null;
	if ((args.timeBefore !== null || args.timeAfter !== null) &&
	    this.ds_timeformat === null) {
		error = new VError('datasource is missing ' +
		    '"timeformat" for "before" and "after" ' +
		    'constraints');
	}

	if (args.timeBefore !== null || args.timeAfter !== null ||
	    args.interval != 'all') {
		if (this.ds_timefield === null) {
			if (error === null) {
				error = new VError('datasource is missing ' +
				    '"timefield" for "before"/"after" ' +
				    'constraints or interval-based indexing');
			}
		} else {
			timefield = sprintf(' --time-field=%s',
			    this.ds_timefield);
		}
	} else {
		timefield = '';
	}

	extraargs = '';
	if (args.timeBefore !== null)
		extraargs += sprintf('--before=%s ',
		    args.timeBefore.toISOString());
	if (args.timeAfter !== null)
		extraargs += sprintf('--after=%s',
		    args.timeAfter.toISOString());

	mod_assertplus.ok(error !== null || timefield !== undefined);
	if (error === null && this.ds_indexpath === null)
		error = new VError('datasource is missing "indexpath"');

	idxconfig = mod_dragnet_impl.indexConfig({
	    'datasource': {
	        'backend': 'manta',
		'filter': this.ds_filter,
		'datapath': this.ds_datapath
	    },
	    'metrics': args.metrics,
	    'user': process.env['MANTA_USER'],
	    'mtime': new Date()
	}, true);

	if (error === null) {
		inputstream = this.listInputs(this.ds_datapath,
		    this.ds_timeformat, args.timeAfter, args.timeBefore);
		if (inputstream instanceof Error)
			error = inputstream;
	}

	if (error !== null) {
		setImmediate(callback, error);
		return;
	}

	idxconfigpath = mod_path.join(this.ds_indexpath, 'indexconfig.json');
	assetpath = this.assetpath(args.assetroot);
	job = new MantaJobBuilder();
	job.name('dragnet index');
	job.phase({
	    'type': 'map',
	    'assets': [ assetpath, idxconfigpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_INDEX0, this.ds_format, timefield,
	        idxconfigpath, args.interval, extraargs)
	});

	job.phase({
	    'type': 'reduce',
	    'assets': [ assetpath, idxconfigpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_INDEX_REDUCE, idxconfigpath,
	        args.interval, this.ds_indexpath)
	});

	log = this.ds_log;
	manta = this.ds_manta;
	mod_vasync.waterfall([
	    function doMkdirp(stepcb) {
		log.debug('mkdirp "%s"', self.ds_indexpath);
		manta.mkdirp(self.ds_indexpath,
		    function (err) { stepcb(err); });
	    },

	    function doPutIndexConfig(stepcb) {
		var passthru;
		passthru = new mod_stream.PassThrough();
		log.debug('put "%s"', idxconfigpath);
		manta.put(idxconfigpath, passthru,
		    function (err) { stepcb(err); });
		passthru.write(JSON.stringify(idxconfig));
		passthru.end();
	    },

	    function doRunJob(stepcb) {
		var jobarg = {
		    'assetpath': assetpath,
		    'inputstream': inputstream,
		    'builder': job
		};

		if (args.dryRun)
			self.jobDryRun(jobarg, stepcb);
		else
			self.jobRunToEnd(jobarg, null, stepcb);
	    }
	], callback);
};

/*
 * Generate the list of inputs for a job.  Given a Manta path ("root"), a time
 * format string ("timeformat"), and optional before/after constraints, return a
 * MantaFinder stream that emits objects for each Manta object that should be
 * searched.
 */
DatasourceManta.prototype.listInputs = function (root, timeformat, start, end)
{
	var filter, finder;

	filter = this.findFilterFunction(root, timeformat, start, end);
	if (filter instanceof Error)
		return (filter);

	finder = new MantaFinder({
	    'log': this.ds_log.child({ 'component': 'find' }),
	    'manta': this.ds_manta,
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
DatasourceManta.prototype.findFilterFunction = function (root, formatstr, start,
    end)
{
	var filter;

	if (start === null && end === null)
		return (FINDFILTER_TRUE);

	filter = mod_timeutil.createTimeStringFilter(formatstr);
	if (filter instanceof Error)
		return (new VError(filter, 'bad time format on datasource'));

	return (function filterPathByTime(path) {
		var tail;
		mod_assertplus.ok(mod_jsprim.startsWith(path, root));
		tail = path.substr(root.length);
		return (filter.rangeContains(start, end, tail));
	});
};

DatasourceManta.prototype.assetpath = function (assetroot)
{
	mod_assertplus.string(assetroot);
	return (mod_path.join(assetroot,
	    sprintf('dragnet-%s.tgz', DRAGNET_VERSION)));
};

/*
 * Execute a job dry run, which just means printing out the job definition and
 * inputs.
 */
DatasourceManta.prototype.jobDryRun = function (job, callback)
{
	console.error(job.builder.json(true));
	console.error('\nInputs:');
	job.inputstream.on('data', function (chunk) {
		console.error(chunk.path);
	});
	job.inputstream.on('end', callback);
};

/*
 * Submit a Manta job, wait for it to complete, and dump the contents of the
 * outputs to "outstream".  This function makes sure that the dragnet asset is
 * present at "assetpath" before submitting the job.
 */
DatasourceManta.prototype.jobRunToEnd = function (job, outstream, jobcb)
{
	var builder, inputstream, assetpath;
	var log, manta, barrier, inputs;

	builder = job.builder;
	inputstream = job.inputstream;
	assetpath = job.assetpath;
	log = this.ds_log;
	manta = this.ds_manta;
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

		if (outstream === null) {
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
		if (outstream === null) {
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
			parser.pipe(outstream);
			callback();
		});
	    }
	], function (err) {
		if (err && outstream !== null)
			outstream.emit('error', err);
		jobcb(err);
	});
};

/*
 * [public] Query a set of indexes.  Arguments are the same as scan(), plus:
 *
 *     interval		force which index to use (same values as for build())
 */
DatasourceManta.prototype.query = function (args)
{
	var query, findparams, manta, idxconfigpath, rv;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.interval, 'args.interval');
	mod_assertplus.bool(args.dryRun, 'args.dryRun');
	mod_assertplus.string(args.assetroot, 'args.assetroot');

	if (this.ds_indexpath === null)
		return (mod_dragnet_impl.asyncError(
		    new VError('datasource is missing "indexpath"')));

	query = args.query;
	findparams = mod_dragnet_impl.indexFindParams({
	    'indexpath': this.ds_indexpath,
	    'interval': args.interval,
	    'timeAfter': query.qc_after,
	    'timeBefore': query.qc_before
	});
	if (findparams instanceof Error)
		return (mod_dragnet_impl.asyncError(findparams));

	manta = this.ds_manta;
	idxconfigpath = mod_path.join(this.ds_indexpath, 'indexconfig.json');
	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	}));
	mod_vasync.waterfall([
	    function doFetchIndexConfig(callback) {
		manta.get(idxconfigpath, function (err, stream) {
			callback(err, stream);
		});
	    },
	    function doReadIndexConfig(stream, callback) {
		mod_streamutil.readToEndJson(stream, callback);
	    },
	    function doCheckQuerySatisfiable(cfg, callback) {
		callback(self.checkSatisfiable(cfg, query));
	    },
	    function doSetupJob(callback) {
		var jobinfo, assetpath, inputstream, job;
		assetpath = self.assetpath(args.assetroot);
		inputstream = self.listInputs(findparams.root,
		    findparams.timeformat, findparams.after, findparams.before);
		if (inputstream instanceof Error) {
			callback(inputstream);
			return;
		}

		job = new MantaJobBuilder();
		job.name('dragnet query');
		job.phase({
		    'type': 'map',
		    'assets': [ assetpath ],
		    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
		    'exec': sprintf(JOBTPL_EXEC_QUERY0, self.ds_indexpath) +
		        ' ' + self.queryToCliArgs(query, false, false).join(' ')
		});

		job.phase({
		    'type': 'reduce',
		    'memory': 2048,
		    'assets': [ assetpath ],
		    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
		    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner', '') +
		        ' ' + self.queryToCliArgs(query, true, false).join(' ')
		});

		jobinfo = {
		    'assetpath': assetpath,
		    'builder': job,
		    'inputstream': inputstream
		};

		if (args.dryRun)
			self.jobDryRun(jobinfo, callback);
		else
			self.jobRunToEnd(jobinfo, rv, callback);
	    }
	], function (err) {
		if (err) {
			rv.emit('error', err);
			return;
		}

		if (args.dryRun)
			rv.end();
	});

	return (rv);
};

/*
 * Check whether the given query can be serviced by the specified index.
 */
DatasourceManta.prototype.checkSatisfiable = function (idxconfig, query)
{
	console.error('note: skipping index check (not yet implemented)');
	return (null);
};

/*
 * [public] See DatasourceFile.
 */
DatasourceManta.prototype.indexScan = function ()
{
	return (mod_dragnet_impl.asyncError(
	    new VError('index scan not supported for Manta backend')));
};

/*
 * [public] See DatasourceFile.
 */
DatasourceManta.prototype.indexRead = function ()
{
	return (mod_dragnet_impl.asyncError(
	    new VError('index read not supported for Manta backend')));
};

/*
 * Given a query, return an array of "dn" command-line arguments that represent
 * the same query.  These are already quoted and escaped as necessary -- they
 * could be joined with whitespace and safely appended directly to a command
 * line.
 */
DatasourceManta.prototype.queryToCliArgs = function queryToCliArgs(query,
    subsequent, usesourcefilter)
{
	var rv, str, fields, filter;

	rv = [];
	if (!subsequent) {
		filter = mod_dragnet_impl.filterAnd(
		    usesourcefilter ? this.ds_filter : null,
		    query.qc_filter);

		if (filter !== null) {
			str = JSON.stringify(filter);
			rv.push(sprintf('--filter \'%s\'', str));
		}

		if (query.qc_before !== null) {
			mod_assertplus.ok(query.qc_after !== null);
			str = query.qc_before.toISOString();
			rv.push(sprintf('--before "%s"', str));
			str = query.qc_after.toISOString();
			rv.push(sprintf('--after "%s"', str));
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
		    values.join(',')));
	});

	if (fields.length > 0)
		/* XXX escape commas, semicolons, and spaces in field names? */
		rv.push(sprintf('--breakdowns=%s', fields.join(',')));

	return (rv);
};

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
