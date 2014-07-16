/*
 * lib/time-util.js: date/time related helper functions
 */

var mod_assert = require('assert');
var VError = require('verror');

exports.parseStrftimePattern = parseStrftimePattern;
exports.createTimeStringFilter = createTimeStringFilter;

/* Supported conversion specifiers */
var spConversions = {
    'Y': true,
    'm': true,
    'd': true,
    'H': true
};

/*
 * Tokenize a strftime(3c)-like pattern string.  Returns an array describing how
 * to construct instances of a pattern string for a given Date.  Each entry in
 * the array contains "kind", which is either:
 *
 *     "str"				This chunk represents a plain string
 *     					taken directly from the underlying
 *     					pattern string.
 *
 *     "Y", "m", "d", or "H"		This chunk represents the corresponding
 *     					strftime(3C) conversion of a date.  So
 *     					"Y" might be expanded to "2014".
 */
function parseStrftimePattern(pattern)
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

		if (!spConversions.hasOwnProperty(conversion))
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

/*
 * A TimeStringFilter is used to filter out strings based on the timestamps that
 * those strings represent.  The prototypical example is that you have a
 * directory tree organized by date (e.g., YYYY/MM/DD/HH) and you want to scan
 * the tree for files corresponding to a certain range of dates.  Importantly,
 * if you're scanning between 2014-05-03 and 2014-05-10, you want to eliminate
 * the entire "2013" directory without descending into it, so we need to be able
 * to prune based on partial applications.
 */
function createTimeStringFilter(pattern)
{
	var tokenized, kind, nbackrefs, backrefs, partials, i;
	var resource;

	tokenized = parseStrftimePattern(pattern);
	if (tokenized instanceof Error)
		return (tokenized);

	nbackrefs = 0;
	backrefs = {};
	partials = [];
	resource = '';
	for (i = 0; i < tokenized.length; i++) {
		kind = tokenized[i].kind;
		if (kind == 'str') {
			/* XXX escape regex metacharacters */
			resource += tokenized[i].value;
			continue;
		}

		/*
		 * Allow repetition of a conversion specifier, but the value
		 * must exactly match the value for the first apperance of the
		 * specifier.
		 */
		if (backrefs.hasOwnProperty(kind)) {
			resource += '\\' + backrefs[kind];
			continue;
		}

		/*
		 * For this to make any sense, the conversion specifiers in the
		 * pattern must be sorted from larger buckets (e.g., year) to
		 * smaller (e.g., hour), and it's important that if any
		 * specifier is present (e.g., "%d" for day-of-month), then all
		 * larger buckets are also specified.
		 */
		if (kind == 'm' && !backrefs.hasOwnProperty('Y'))
			return (new VError('"%Y" must appear before "%m"'));
		if (kind == 'd' && !backrefs.hasOwnProperty('m'))
			return (new VError('"%m" must appear before "%d"'));
		if (kind == 'H' && !backrefs.hasOwnProperty('d'))
			return (new VError('"%H" must appear before "%d"'));

		backrefs[kind] = ++nbackrefs;
		if (kind == 'Y')
			resource += '(\\d\\d\\d\\d)';
		else
			resource += '(\\d\\d)';
		partials.unshift({
		    'regexp': new RegExp(resource),
		    'maxbackref': nbackrefs
		});
	}

	/*
	 * At this point, "partials" is a list of regular expression sources
	 * that we can use to process all partial matches of a string.  It's
	 * sorted from the most specific match to the most general.  For
	 * example, if the format string is "%Y/%m/%d', then "partials" would
	 * have regular expression strings:
	 *
	 * partials[0]: (\d\d\d\d)/(\d\d)/(\d\d)  (matches year, month, and day)
	 * partials[1]: (\d\d\d\d)/(\d\d)         (matches year and month only)
	 * partials[2]: (\d\d\d\d)                (matches year only)
	 *
	 * At this point, "backrefs" indicates for each conversion specifier
	 * in the conversion string the numeric backreference of the first
	 * appearance of that specifier in the most specific regular expression.
	 * In this example, backrefs.Y = 1, backrefs.m = 2, and backrefs.d = 3,
	 * which tells us that if any given pattern matches any of the partial
	 * regexes, then the year can be found at backreference 1, the month at
	 * backreference 2, and so on.
	 *
	 * Putting these together, we have an algorithm for figuring out the
	 * minimum and maximum dates represented by an arbitrary input string
	 * matching any portion of the pattern.  For example, consider input
	 * string "2013/03".  We apply the partial regexes in order until one
	 * matches.  In this case, partials[1] is the first one that matches.
	 * We pull out the fields present (year and month) based on the
	 * corresponding backreferences (1 and 2).  Now that we've extracted a
	 * year and month from the input string, it's easy to figure out the
	 * earliest and latest timestamps corresponding to the string.
	 */
	return (new TimeStringFilter(pattern, tokenized, partials, backrefs));
}

function TimeStringFilter(pattern, tokenized, partials, backrefs)
{
	this.sc_pattern = pattern;
	this.sc_tokenized = tokenized;
	this.sc_partials = partials;
	this.sc_backrefs = backrefs;
}

TimeStringFilter.prototype.rangeContains = function (start, end, input)
{
	var pi, match, maxref, year, value, strstart, strend;

	for (pi = 0; pi < this.sc_partials.length; pi++) {
		match = this.sc_partials[pi].regexp.exec(input);
		if (match !== null)
			break;
	}

	if (pi == this.sc_partials.length) {
		/*
		 * With no time-related constraints implied by the string, it
		 * matches all dates and times.
		 */
		return (true);
	}

	/*
	 * Compute the minimum time represented by this string by creating a
	 * Date object for the matching year (which represents
	 * $YEAR-01-01T00:00:000Z) and setting whichever fields are present in
	 * the string (month, day, or hour).
	 */
	maxref = this.sc_partials[pi].maxbackref;
	mod_assert.ok(this.sc_backrefs.Y && this.sc_backrefs.Y <= maxref);
	year = match[this.sc_backrefs.Y];
	strstart = new Date(year);
	mod_assert.ok(!isNaN(strstart.getTime()));
	mod_assert.equal(strstart.getUTCFullYear().toString(), year);

	if (this.sc_backrefs.m && this.sc_backrefs.m <= maxref) {
		value = parseInt(match[this.sc_backrefs.m], 10);
		mod_assert.ok(!isNaN(value));
		strstart.setUTCMonth(value - 1);

		if (this.sc_backrefs.d && this.sc_backrefs.d <= maxref) {
			value = parseInt(match[this.sc_backrefs.d], 10);
			mod_assert.ok(!isNaN(value));
			strstart.setUTCDate(value);

			if (this.sc_backrefs.H &&
			    this.sc_backrefs.H <= maxref) {
				value = parseInt(match[this.sc_backrefs.H], 10);
				mod_assert.ok(!isNaN(value));
				strstart.setUTCHours(value);
			}
		}
	}

	if (end !== null && strstart.getTime() >= end.getTime())
		return (false);

	/*
	 * Compute the end time of the bucket by adding the size of the bucket.
	 */
	strend = new Date(strstart.getTime());
	if (this.sc_backrefs.H && this.sc_backrefs.H <= maxref)
		strend.setUTCHours(strstart.getUTCHours() + 1);
	else if (this.sc_backrefs.d && this.sc_backrefs.d <= maxref)
		strend.setUTCDate(strstart.getUTCDate() + 1);
	else if (this.sc_backrefs.m && this.sc_backrefs.m <= maxref)
		strend.setUTCMonth(strstart.getUTCMonth() + 1);
	else {
		mod_assert.ok(this.sc_backrefs.Y &&
		    this.sc_backrefs.Y <= maxref);
		strend.setUTCFullYear(strstart.getUTCFullYear() + 1);
	}

	if (start !== null && strend.getTime() <= start.getTime())
		return (false);

	return (true);
};
