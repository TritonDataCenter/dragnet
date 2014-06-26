/*
 * path-enum: given a string with wildcards for fields of a timestamp (e.g.,
 * year, month, day, and hour) and a start and end timestamp, enumerate unique
 * values of the string for all times between the start and end.
 */

var mod_assertplus = require('assert-plus');
var mod_stream = require('stream');
var mod_streamutil = require('./stream-util');
var mod_util = require('util');
var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

/*
 * Public interface
 */
exports.createPathEnumerator = createPathEnumerator;

/*
 * These are the approximate values of the "unit" for each of the supported
 * conversion specifiers.  These don't really have to be accurate.  They just
 * need to sort correctly.  It's easiest to define them roughly in terms of the
 * smallest unit.
 */
var peConversions = {
	'Y': 365 * 24,	/* year */
	'm': 30 * 24,	/* month */
	'd': 24,	/* day */
	'H': 1		/* hour */
};

/*
 * Arguments:
 *
 *    pattern		string with wildcards for the year, month, and day
 *    			The only supported wildcards are "%Y", "%m", "%d", and
 *    			"%H", which correspond to the conversion specifiers
 *    			accepted by strftime(3C).
 *
 *    timeStart		Date representing the start time (inclusive)
 *
 *    timeEnd		Date representing the end time (exclusive)
 *
 *    [streamOptions]	arbitrary Node-API stream options
 *
 * Emits unique strings matching "pattern" for all time values between timeStart
 * and timeEnd.  A prototypical example would be:
 *
 *    {
 *        "pattern": "%Y/%m/%d/%H",
 *        "timeStart": new Date("2014-06-26T20:00:00Z"),
 *        "timeEnd": new Date("2014-06-27T04:00:00Z")
 *    }
 *
 * which would emit values:
 *
 *     "2014/06/26/20", "2014/06/26/21", "2014/06/26/22", "2014/06/26/23",
 *     "2014/06/27/00", "2014/06/27/01", "2014/06/27/02", "2014/06/27/03"
 *
 * The stream supports flow control.
 */
function createPathEnumerator(args)
{
	var generator;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.pattern, 'args.pattern');
	mod_assertplus.object(args.timeStart, 'args.timeStart');
	mod_assertplus.object(args.timeEnd, 'args.timeEnd');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	if (isNaN(args.timeStart.getTime()))
		return (new Error('"timeStart" is not a valid date'));

	if (isNaN(args.timeEnd.getTime()))
		return (new Error('"timeEnd" is not a valid date'));

	if (args.timeStart.getTime() > args.timeEnd.getTime())
		return (new Error('"timeStart" may not be after "timeEnd"'));

	generator = parsePattern(args.pattern);
	if (generator instanceof Error)
		return (generator);

	return (new PathEnumerator({
	    'pattern': args.pattern,
	    'timeStart': args.timeStart,
	    'timeEnd': args.timeEnd,
	    'streamOptions': args.streamOptions,
	    'generator': generator
	}));
}

/*
 * Tokenize the pattern string.  Returns an array describing how to construct
 * instances of a pattern string for a given Date.  Each entry in the array
 * contains "kind", which is either:
 *
 *     "str"				This chunk represents a plain string
 *     					taken directly from the underlying
 *     					pattern string.
 *
 *     "Y", "m", "d", or "H"		This chunk represents the corresponding
 *     					strftime(3C) conversion of a date.  So
 *     					"Y" might be expanded to "2014".
 */
function parsePattern(pattern)
{
	var rv, i, conversion, last;

	rv = [];
	last = 0;
	while ((i = pattern.indexOf('%', last)) != -1) {
		if (i == pattern.length - 1)
			return (new VError('unexpected "%%" at char %d',
			    i + 1));

		if (i !== 0) {
			rv.push({
			    'kind': 'str',
			    'value': pattern.substr(last, i - last)
			});
		}

		conversion = pattern.charAt(i + 1);
		if (conversion == '%') {
			rv.push({
			    'kind': 'str',
			    'value': '%'
			});
			last = i + 2;
			continue;
		}

		if (!peConversions.hasOwnProperty(conversion))
			return (new VError('unsupported conversion "%%%s" at ' +
			    'char %d', conversion, i + 1));

		rv.push({ 'kind': conversion });
		last = i + 2;
	}

	if (last < pattern.length) {
		rv.push({
		    'kind': 'str',
		    'value': pattern.substr(last)
		});
	}

	return (rv);
}

function PathEnumerator(args)
{
	var streamoptions, minconv, minunit;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.pattern, 'args.pattern');
	mod_assertplus.object(args.timeStart, 'args.timeStart');
	mod_assertplus.object(args.timeEnd, 'args.timeEnd');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');
	mod_assertplus.arrayOfObject(args.generator, 'args.generator');

	streamoptions = mod_streamutil.streamOptions(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 20 });
	mod_stream.Readable.call(this, streamoptions);

	this.pe_pattern = args.pattern;
	this.pe_generator = args.generator;
	this.pe_start = new Date(args.timeStart.getTime());
	this.pe_end = new Date(args.timeEnd.getTime());
	this.pe_next = new Date(this.pe_start.getTime());
	this.pe_minunit = null;
	this.pe_ended = false;

	/*
	 * Figure out how to increment each date to get to the next one.  Note
	 * that this isn't just a matter of incrementing the millisecond value
	 * by a certain amount.  To increment by a month, we actually want to
	 * bump the "month" of the current date.  If we increment by 28 or 31
	 * days or something, we can eventually produce incorrect output.
	 *
	 * All we really want here is to find the smallest-unit conversion
	 * that's used.  These values don't have to be accurate.  They just need
	 * to sort correctly.
	 */
	minunit = Infinity;
	this.pe_generator.forEach(function (entry) {
		var unit;

		if (entry.kind == 'str')
			return;

		mod_assertplus.ok(peConversions.hasOwnProperty(entry.kind));
		unit = peConversions[entry.kind];
		if (unit < minunit) {
			minunit = unit;
			minconv = entry.kind;
		}
	});

	/*
	 * The initial value must be aligned with the smallest unit in the
	 * conversion string, and we round down.  Since we don't support
	 * anything smaller than hours, we can always set minutes, seconds, and
	 * milliseconds to zero.
	 */
	this.pe_next.setUTCMinutes(0, 0, 0);

	if (minunit !== Infinity) {
		this.pe_minunit = minconv;

		switch (minconv) {
		case 'Y':
			this.pe_next.setUTCMonth(0);
			/* jsl:fallthru */

		case 'm':
			this.pe_next.setUTCDate(1);
			/* jsl:fallthru */

		case 'd':
			this.pe_next.setUTCHours(0);
			break;
		}
	}
}

mod_util.inherits(PathEnumerator, mod_stream.Readable);

PathEnumerator.prototype._read = function ()
{
	var nextvalue;

	if (this.pe_next === null) {
		mod_assertplus.ok(!this.pe_ended);
		this.pe_ended = true;
		this.push(null);
		return;
	}

	for (;;) {
		nextvalue = this.nextValue();
		if (!this.push(nextvalue))
			break;

		if (nextvalue === null)
			break;
	}
};

PathEnumerator.prototype.nextValue = function ()
{
	var rv;

	if (this.pe_next === null)
		return (null);

	rv = this.expand(this.pe_next);
	this.increment();
	return (rv);
};

PathEnumerator.prototype.expand = function (timestamp)
{
	return (this.pe_generator.map(function (entry) {
		if (entry.kind == 'str')
			return (entry.value);

		switch (entry.kind) {
		case 'Y':
			return (timestamp.getUTCFullYear());
		case 'm':
			return (sprintf('%02d', timestamp.getUTCMonth() + 1));
		case 'd':
			return (sprintf('%02d', timestamp.getUTCDate()));
		default:
			mod_assertplus.equal(entry.kind, 'H');
			return (sprintf('%02d', timestamp.getUTCHours()));
		}
	}).join(''));
};

PathEnumerator.prototype.increment = function ()
{
	mod_assertplus.ok(this.pe_next !== null);

	if (this.pe_minunit === null) {
		this.pe_next = null;
		return;
	}

	switch (this.pe_minunit) {
	case 'Y':
		this.pe_next.setUTCFullYear(this.pe_next.getUTCFullYear() + 1);
		break;

	case 'm':
		/*
		 * This is only correct because we enforce alignment of the
		 * starting value in the constructor.  Otherwise, this could
		 * result in skipping months with fewer days than the
		 * day-of-month with which the start time was constructed.
		 */
		this.pe_next.setUTCMonth(this.pe_next.getUTCMonth() + 1);
		break;

	case 'd':
		this.pe_next.setUTCDate(this.pe_next.getUTCDate() + 1);
		break;

	default:
		mod_assertplus.equal(this.pe_minunit, 'H');
		this.pe_next.setUTCHours(this.pe_next.getUTCHours() + 1);
		break;
	}

	if (this.pe_next.getTime() >= this.pe_end.getTime())
		this.pe_next = null;
};
