/*
 * lib/source-manta.js: Manta-based Dragnet backend.
 */

var mod_assertplus = require('assert-plus');
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
var FINDFILTER_TRUE = function () { return (true); };

/*
 * Given a query, return an array of "dn" command-line arguments that represent
 * the same query.  These are already quoted and escaped as necessary -- they
 * could be joined with whitespace and safely appended directly to a command
 * line.
 */
function queryToCliArgs(query, subsequent)
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
					    shEscape(key), shEscape(fc[key])));
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
		rv.push(sprintf('--breakdowns=%s', fields.join(',')));

	return (rv);
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
	if (/[^a-zA-Z0-9 $|_\-\.]/.test(str))
		throw (new VError('unsupported token: "$s"', str));
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
}

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
 */
MantaDataSource.prototype.scan = function (args)
{
	var query, error, builder, rv, inputstream, assetpath;
	var nextrareducers = 0;
	var nreducercount = 2;
	var i;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.query, 'args.query');
	mod_assertplus.string(args.format, 'args.format');

	rv = mod_vstream.wrapTransform(new mod_stream.PassThrough({
	    'objectMode': true,
	    'highWaterMark': 0
	}));

	query = args.query;
	if (query.qc_before !== null || query.qc_after !== null)
		mod_assertplus.string(args.timeFormat, 'args.timeFormat');

	/*
	 * Validate the format name, since we're going to be inserting it into
	 * the script directly.
	 */
	error = mod_dragnet_impl.parserFor(args.format);
	if (error instanceof Error) {
		setImmediate(function () { rv.emit('error', error); });
		return (rv);
	}

	mod_assertplus.ok(process.env['MANTA_USER']);
	assetpath = sprintf('/%s/stor/dragnet.tgz', process.env['MANTA_USER']);
	builder = new JobBuilder();
	builder.name('dragnet scan');
	builder.phase({
	    'type': 'map',
	    'assets': [ assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, args.format) + ' ' +
	        queryToCliArgs(query, false).join(' ') + ' /dev/stdin'
	});

	for (i = 0; i < nextrareducers; i++) {
		builder.phase({
		    'type': 'reduce',
		    'count': nreducercount,
		    'assets': [ assetpath ],
		    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
		    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner') + ' ' +
			queryToCliArgs(query, true).join(' ') + ' /dev/stdin'
		});
	}

	builder.phase({
	    'type': 'reduce',
	    'assets': [ assetpath ],
	    'init': sprintf(JOBTPL_INIT_UNPACK_ASSET, assetpath),
	    'exec': sprintf(JOBTPL_EXEC_SCAN0, 'json-skinner') + ' ' +
	        queryToCliArgs(query, true).join(' ') + ' /dev/stdin'
	});

	mod_assertplus.string(this.mds_dataroot);
	inputstream = this.listInputs(this.mds_dataroot,
	    args.timeFormat, query);
	if (inputstream instanceof Error) {
		setImmediate(function () { rv.emit('error', inputstream); });
		return (rv);
	}

	inputstream.on('error', rv.emit.bind(rv, 'error'));

	if (args.dryRun) {
		console.error(builder.json(true));
		console.error('\nInputs:');
		inputstream.on('data', function (chunk) {
			console.error(chunk.path + '\n');
		});
		inputstream.on('end', function () { rv.end(); });
		return (rv);
	}

	this.runJobToEnd(assetpath, builder, inputstream, rv);
	return (rv);
};

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

MantaDataSource.prototype.runJobToEnd = function (assetpath, builder,
    inputstream, rv)
{
	var log = this.mds_log;
	var manta = this.mds_manta;
	var barrier = mod_vasync.barrier();
	var inputs = [];

	barrier.start('collect inputs');
	barrier.start('submit job');
	inputstream.on('data', function (chunk) { inputs.push(chunk.path); });
	inputstream.on('end', function () { barrier.done('collect inputs'); });

	mod_vasync.waterfall([
	    function jobMakeAsset(callback) {
		var dir, fstream, tarpacker, zipper;

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
		var job = builder.job();
		log.debug('submitting job', job);
		manta.createJob(job, function (err, jobid) {
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

function JobBuilder()
{
	this.jb_name = '';
	this.jb_phases = [];
}

JobBuilder.prototype.name = function (name)
{
	mod_assertplus.string(name, 'name');
	this.jb_name = name;
};

JobBuilder.prototype.phase = function (phase)
{
	mod_assertplus.object(phase, 'phase');
	mod_assertplus.string(phase.type, 'phase.type');
	mod_assertplus.optionalArrayOfString(phase.assets, 'phase.assets');
	mod_assertplus.optionalString(phase.init, 'phase.init');
	mod_assertplus.string(phase.exec, 'phase.exec');
	this.jb_phases.push(phase);
};

JobBuilder.prototype.job = function ()
{
	return ({
	    'name': this.jb_name,
	    'phases': this.jb_phases
	});
};

JobBuilder.prototype.json = function (pretty)
{
	return (JSON.stringify(this.job(), null, pretty ? '    ' : ''));
};
