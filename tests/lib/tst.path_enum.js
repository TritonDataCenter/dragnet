/*
 * tst.path_enum.js: test the path enumerator (see lib/path-enum.js)
 */

var mod_assert = require('assert');
var mod_pathenum = require('../../lib/path-enum');
var mod_vasync = require('vasync');
var VError = require('verror');

var test_cases = [
/*
 * Invalid arguments (operational errors, not programming errors)
 */
{
    'label': 'invalid pattern: ends with %',
    'pattern': 'my_pattern%',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'error': /^unexpected "%" at char 11$/
}, {
    'label': 'invalid pattern: unsupported conversion',
    'pattern': 'my_pattern%T',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'error': /^unsupported conversion "%T" at char 11$/
}, {
    'label': 'invalid start time',
    'pattern': 'my_pattern%T',
    'range': [ 'q', '2010-01-10T00:00:00Z' ],
    'error': /^"timeStart" is not a valid date$/
}, {
    'label': 'invalid end time',
    'pattern': 'my_pattern%T',
    'range': [ '2010-01-10T00:00:00Z', 'q' ],
    'error': /^"timeEnd" is not a valid date$/
}, {
    'label': 'invalid dates',
    'pattern': 'my_pattern%T',
    'range': [ '2010-01-11T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'error': /^"timeStart" may not be after "timeEnd"$/
},

/*
 * Patterns requiring no time-based expansion
 */
{
    'label': 'pattern that does not depend on the date',
    'pattern': 'my_pattern',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'values': [ 'my_pattern' ]
}, {
    'label': 'pattern with "%"',
    'pattern': 'my_%%pattern',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'values': [ 'my_%pattern' ]
}, {
    'label': 'pattern starts with "%"',
    'pattern': 'my_pattern%%',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'values': [ 'my_pattern%' ]
}, {
    'label': 'pattern ends with "%"',
    'pattern': 'my_pattern%%',
    'range': [ '2010-01-01T00:00:00Z', '2010-01-10T00:00:00Z' ],
    'values': [ 'my_pattern%' ]
},

/*
 * Year-level patterns
 */
{
    'label': 'year-level pattern',
    'pattern': '%Y',
    'range': [ '2010-12-03T01:23:45.678Z', '2013-01-01T00:00:00.000' ],
    'values': [ '2010', '2011', '2012' ]
},{
    'label': 'year-level pattern (reaches into next year)',
    'pattern': '%Y',
    'range': [ '2010-01-01T00:00:00.000Z', '2013-01-01T00:00:00.001' ],
    'values': [ '2010', '2011', '2012', '2013' ]
}, {
    'label': 'smallest range with year-level pattern',
    'pattern': '%Y',
    'range': [ '2014-02-01T00:00:00.000Z', '2014-02-01T00:00:00.000Z' ],
    'values': [ '2014' ]
}, {
    'label': 'smallest range spanning two years with year-level pattern',
    'pattern': '%Y',
    'range': [ '2014-12-31T23:59:59.999Z', '2015-01-01T00:00:00.001Z' ],
    'values': [ '2014', '2015' ]
},

/*
 * Month-level patterns.  These are tricky because months have different numbers
 * of days.
 */
{
    'label': 'month-only pattern', /* a little odd */
    'pattern': '%m',
    'range': [ '2010-06-01T00:00:00Z', '2012-08-01T00:00:00Z' ],
    'values': [ '06', '07', '08', '09', '10', '11', '12', '01', '02', '03',
        '04', '05', '06', '07', '08', '09', '10', '11', '12', '01', '02', '03',
	'04', '05', '06', '07' ]
},
{
    'label': 'basic year-and-month pattern',
    'pattern': '%Y-%m',
    'range': [ '2010-06-01T00:00:00Z', '2012-08-01T00:00:00Z' ],
    'values': [ '2010-06', '2010-07', '2010-08', '2010-09', '2010-10',
        '2010-11', '2010-12', '2011-01', '2011-02', '2011-03', '2011-04',
	'2011-05', '2011-06', '2011-07', '2011-08', '2011-09', '2011-10',
	'2011-11', '2011-12', '2012-01', '2012-02', '2012-03', '2012-04',
	'2012-05', '2012-06', '2012-07' ]
}, {
    'label': 'year-and-month pattern starting from day 30',
    'pattern': '%Y-%m',
    'range': [ '2010-10-30T00:00:00Z', '2011-05-01T00:00:00Z' ],
    'values': [ '2010-10', '2010-11', '2010-12', '2011-01', '2011-02',
        '2011-03', '2011-04' ]
}, {
    'label': 'smallest range with year-and-month pattern',
    'pattern': '%Y/%m',
    'range': [ '2014-02-01T00:00:00.000Z', '2014-02-01T00:00:00.000Z' ],
    'values': [ '2014/02' ]
}, {
    'label': 'smallest range spanning two months with year-and-month pattern',
    'pattern': '%Y/%m',
    'range': [ '2014-01-31T23:59:59.999Z', '2014-02-01T00:00:00.001Z' ],
    'values': [ '2014/01', '2014/02' ]
},

/*
 * Day-level patterns.
 */
{
    'label': 'day-only pattern', /* a little odd */
    'pattern': '%d',
    'range': [ '2010-06-12T03:05:06Z', '2010-06-18T00:00:00Z' ],
    'values': [ '12', '13', '14', '15', '16', '17' ]
}, {
    'label': 'basic year-month-day pattern',
    'pattern': 'year_%Y/month_%m/day_%d/some/other/stuff',
    'range': [ '2014-02-26', '2014-03-03' ],
    'values': [
	'year_2014/month_02/day_26/some/other/stuff',
	'year_2014/month_02/day_27/some/other/stuff',
	'year_2014/month_02/day_28/some/other/stuff',
	'year_2014/month_03/day_01/some/other/stuff',
	'year_2014/month_03/day_02/some/other/stuff'
    ]
}, {
    'label': 'smallest range with MD pattern',
    'pattern': '%m/%d',
    'range': [ '2014-02-01T00:00:00.000Z', '2014-02-01T00:00:00.000Z' ],
    'values': [ '02/01' ]
}, {
    'label': 'smallest range spanning two days with YMD pattern',
    'pattern': '%m/%d',
    'range': [ '2014-01-31T23:59:59.999Z', '2014-02-01T00:00:00.001Z' ],
    'values': [ '01/31', '02/01' ]
},

/*
 * Hour-level patterns.
 */
{
    'label': 'hour-only pattern', /* a little odd */
    'pattern': '%H',
    'range': [ '2010-06-12T03:05:06Z', '2010-06-12T09:00:00Z' ],
    'values': [ '03', '04', '05', '06', '07', '08' ]
}, {
    'label': 'basic year-month-day-hour pattern',
    'pattern': '%Y/%m/%d/%H',
    'range': [ '2014-02-28T20:00:00Z', '2014-03-01T04:00:00Z' ],
    'values': [
        '2014/02/28/20', '2014/02/28/21', '2014/02/28/22', '2014/02/28/23',
        '2014/03/01/00', '2014/03/01/01', '2014/03/01/02', '2014/03/01/03'
    ]
}, {
    'label': 'smallest range with DH pattern',
    'pattern': '%d/%H',
    'range': [ '2014-02-01T00:00:00.000Z', '2014-02-01T00:00:00.000Z' ],
    'values': [ '01/00' ]
}, {
    'label': 'smallest range spanning two hours with YMD pattern',
    'pattern': '%d/%H',
    'range': [ '2014-01-31T23:59:59.999Z', '2014-02-01T00:00:00.001Z' ],
    'values': [ '31/23', '01/00' ]
} ];

function main()
{
	mod_vasync.forEachPipeline({
	    'inputs': test_cases,
	    'func': function (testcase, callback) {
		var stream, results;

		process.stderr.write('case: ' + testcase.label + ': ');
		stream = mod_pathenum.createPathEnumerator({
		    'pattern': testcase.pattern,
		    'timeStart': new Date(testcase.range[0]),
		    'timeEnd': new Date(testcase.range[1])
		});

		if (testcase.error) {
			if (!(stream instanceof Error)) {
				console.error('FAIL');
				callback(new VError('expected error ("%s")',
				    testcase.error.source));
				return;
			}

			if (!testcase.error.test(stream.message)) {
				console.error('FAIL');
				callback(new VError('error ("%s") did not ' +
				    'match expected error ("%s")',
				    stream.message, testcase.error.source));
				return;
			}

			console.error('OK ("' + stream.message + '")');
			callback();
			return;
		}

		if (stream instanceof Error) {
			console.error('FAIL');
			callback(new VError(stream, 'unexpected error'));
			return;
		}

		results = [];
		stream.on('data', function (c) { results.push(c); });
		stream.on('end', function () {
			mod_assert.deepEqual(testcase.values, results);
			console.error('OK (%d values)', results.length);
			callback();
		});
	    }
	}, function (err) {
		if (err)
			throw (err);
		console.log('test passed');
	});
}

main();
