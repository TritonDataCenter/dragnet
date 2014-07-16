/*
 * tst.timefilter.js: tests time filter class
 */

var mod_timeutil = require('../../lib/time-util');
var sprintf = require('extsprintf').sprintf;
var pattern1 = 'year-%Y/month-%m/day-%d';
var pattern2 = '%Y/%m/%d/%H';
var testcases = [ {
    /* before-and-after */
    'pattern': pattern1,
    'start': new Date('2014-05-02T12:34:56.789Z'),
    'end': new Date('2014-05-05T17:34:56.789Z'),
    'matching': [
	'junk',
	'year-2014',
	'year-2014/',
	'year-2014/month-05',
	'year-2014/month-05/',
	'year-2014/month-05/day-02',
	'year-2014/month-05/day-02/',
	'year-2014/month-05/day-02/file1',
	'year-2014/month-05/day-02/dir1/file2',
	'year-2014/month-05/day-04/dir1/file2',
	'year-2014/month-05/day-04',
	'year-2014/month-05/day-05/dir1/file2',
	'year-2014/month-05/day-05/dir1/',
	'year-2014/month-05/day-05/dir1',
	'year-2014/month-05/day-05/',
	'year-2014/month-05/day-05',
    ],
    'nonmatching': [
	'year-2013',
	'year-2014/month-04',
	'year-2014/month-05/day-01',
	'year-2014/month-05/day-06',
    ]
}, {
    /* after-only */
    'pattern': pattern1,
    'start': new Date('2014-05-02T12:34:56.789Z'),
    'end': null,
    'matching': [
	'junk',
        'year-2014/month-05/day-02',
        'year-2014/month-05/day-03',
        'year-2014',
	'year-2015',
	'year-2038',
	'year-2038/',
	'year-2038/foobar',
	'year-2038/month-05',
	'year-2038/month-05/day-04',
	'year-2038/month-05/day-08',
	'year-2038/month-05/day-08/anything',
	'year-2038/month-05/day-08/anything/else'
    ],
    'nonmatching': [
        'year-1700',
        'year-1900',
        'year-2000',
        'year-2013/month-05/day-03',
        'year-2014/month-05/day-01'
    ],
}, {
    /* before-only */
    'pattern': pattern1,
    'start': null,
    'end': new Date('2014-05-05T17:34:56.789Z'),
    'matching': [
	'junk',
	'year-1700',
	'year-2014',
	'year-2014/month-05',
	'year-2014/month-05/day-04',
	'year-2014/month-05/day-05',
    ],
    'nonmatching': [
	'year-2014/month-05/day-06',
	'year-2014/month-06/day-03',
	'year-2015/month-05/day-03',
	'year-2015/month-05',
	'year-2015',
    ]
}, {
    /* pattern that includes an hour */
    'pattern': pattern2,
    'start': new Date('2014-05-05T02:23:45.678Z'),
    'end': new Date('2014-06-03T00:23:45.678Z'),
    'matching': [
	'junk',
	'2014',			/* first legit year */
	'2014/',
	'2014/05',		/* first legit month */
	'2014/05/',
	'2014/05/05',		/* first legit day */
	'2014/05/05/',
	'2014/05/05/02',	/* first legit hour */
	'2014/05/05/02/some',
	'2014/05/05/03',
	'2014/05/08/00',
	'2014/06/03/00/some',	/* last legit hour */
	'2014/06/03/00',
	'2014/06/03',		/* last legit day */
	'2014/06/03/',
	'2014/06',		/* last legit month */
	'2014/06/',
	'2014',			/* last legit year */
	'2014/'
    ],
    'nonmatching': [
	/* before */
	'2013/05/08/00',	/* year doesn't match */
	'2014/03/08/00',	/* month doesn't match */
	'2014/05/04/00',	/* day doesn't match */
	'2014/05/05/01',	/* hour doesn't match */

	/* after */
	'2014/06/03/01',	/* hour doesn't match */
	'2014/06/03/01/some',
	'2014/06/04',		/* day doesn't match */
	'2014/06/04/',
	'2014/06/04/00',
	'2014/06/04/00/some',
	'2014/07',		/* month doesn't match */
	'2014/07/',
	'2014/07/04',
	'2014/07/04/',
	'2014/07/04/00',
	'2014/07/04/00/some',
	'2015',			/* year doesn't match */
	'2015/',
	'2015/06',
	'2015/06/',
	'2015/06/01',
	'2015/06/01/',
	'2015/06/01/00',
	'2014/07/04/00/some',
    ]
}, {
   /* endpoint edge cases */
   'pattern': pattern2,
   'start': new Date('2014-05-05T22:00:00Z'),
   'end': new Date('2014-05-06T02:00:00Z'),
   'matching': [
       '2014/05/05/22',
       '2014/05/05/23',
       '2014/05/06/00',
       '2014/05/06/01'
   ],
   'nonmatching': [
       '2014/05/05/21',
       '2014/05/06/02'
   ]
} ];

function errprintf()
{
	var str = sprintf.apply(null, Array.prototype.slice.call(arguments));
	process.stderr.write(str);
}

function main()
{
	var nerrors = 0;

	testcases.forEach(function (testcase) {
		var filter, result;
	
		errprintf('"%s" from %s to %s\n', testcase.pattern,
		    testcase.start === null ? 'null' :
		        testcase.start.toISOString(),
		    testcase.end === null ? 'null' :
		        testcase.end.toISOString());
		filter = mod_timeutil.createTimeStringFilter(testcase.pattern);

		testcase.matching.forEach(function (str) {
			result = filter.rangeContains(
			    testcase.start, testcase.end, str);
			if (!result) {
				errprintf('    FAIL: %-5s "%s"\n', result, str);
				nerrors++;
			} else {
				errprintf('    OK:   %-5s "%s"\n', result, str);
			}
		});

		testcase.nonmatching.forEach(function (str) {
			result = filter.rangeContains(
			    testcase.start, testcase.end, str);
			if (result) {
				errprintf('    FAIL: %-5s "%s"\n', result, str);
				nerrors++;
			} else {
				errprintf('    OK:   %-5s "%s"\n', result, str);
			}
		});

		errprintf('\n');
	});

	process.exit(nerrors === 0 ? 0 : 1);
}

main();
