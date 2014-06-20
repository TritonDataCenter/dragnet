#!/usr/bin/env node

/*
 * Generate sample data that looks roughly like muskie logs.  The goal is to be
 * able to exercise various dragnet cases rather than to look exactly like those
 * logs.
 */

var mod_assert = require('assert');

/*
 * All records will have timestamps linearly increasing between these two times.
 */
var mindate = Date.parse('2014-05-31T21:00:00Z');
var maxdate = Date.parse('2014-05-31T23:59:59Z');
var nrecords;

/*
 * Sample probability distribution for latency-like values.
 */
var dist = [
    /*  P   MIN    MAX */
    [ 0.4,    1,     5 ],
    [ 0.3,   20,    30 ],
    [ 0.1,  100,   200 ],
    [ null, 1024, 4096 ]
];

/*
 * Generate some sample URLs.  This should be a large number, but not so large
 * that we don't get duplicates.
 */
var nurls = 500;
var urls = [];
var i;
for (i = 0; i < nurls; i++)
	urls.push('/random/url/number/' + i);

/*
 * Configuration that we use to generate each record.
 */
var config = [ {
    /* a few small-cardinality discrete fields */
    'name': 'host',
    'values': [ 'ralph', 'janey', 'kearney', 'sherri', 'wendell' ]
}, {
    /* discrete fields that only appear with certain other fields. */
    'parent': 'req',
    'name': 'method',
    'values': [ 'HEAD', 'GET', 'PUT', 'DELETE' ],
    'dependents': {
	'HEAD': [ {
	    'name': 'operation',
	    'values': [ 'headstorage', 'headpublicstorage' ]
	} ],
	'GET': [ {
	    'name': 'operation',
	    'values': [ 'getjoberrors', 'getpublicstorage', 'getstorage' ]
	} ],
	'PUT': [ {
	    'name': 'operation',
	    'values': [ 'putdirectory', 'putpublicobject', 'putobject' ]
	} ],
	'DELETE': [ {
	    'name': 'operation',
	    'values': [ 'deletestorage', 'deletepublicstorage' ]
	} ]
    }
}, {
    /* large set of basically strings */
    'parent': 'req',
    'name': 'url',
    'values': urls
}, {
    /* nullable, optional, nested field */
    'parent': 'req',
    'name': 'caller',
    'values': [ 'admin', 'poseidon', null, undefined ]
}, {
    /* fixed, relatively small set of numeric values */
    'parent': 'res',
    'name': 'statusCode',
    'values': [ 200, 204, 400, 404, 499, 500, 503 ]
}, {
    /* a few reasonable latency-like fields */
    'name': 'latency',
    'probdist': dist
}, {
    'name': 'dataLatency',
    'probdist': dist
}, {
    /* large range of numbers */
    'name': 'dataSize',
    'probdist': [
	[ null, 0, 1024 * 1024 * 1024 ]
    ]
} ];

function usage()
{
	console.error('usage: mktestdata.js NRECORDS');
	process.exit(2);
}

function main()
{
	var j;

	if (process.argv.length > 3)
		usage();

	if (process.argv.length == 3) {
		nrecords = parseInt(process.argv[2], 10);
		if (isNaN(nrecords) || nrecords < 0)
			usage();
	} else {
		nrecords = 1000;
	}

	for (j = 0; j < nrecords; j++)
		console.log(JSON.stringify(makeRecord(config, j)));
}

function makeRecord(c, j)
{
	var rv = {};
	var ts = Math.round((j / nrecords) * (maxdate - mindate) + mindate);

	rv['time'] = new Date(ts);
	c.forEach(function (propconf) {
		doProp(rv, propconf);
	});

	return (rv);
}

function doProp(obj, propconf)
{
	var rand, cmrand, value, which, j, pconf, o;

	if (propconf.parent) {
		if (!obj[propconf.parent])
			obj[propconf.parent] = {};
		o = obj[propconf.parent];
	} else {
		o = obj;
	}

	if (propconf.values) {
		mod_assert.ok(Array.isArray(propconf.values));
		which = Math.floor(Math.random() * propconf.values.length);
		value = propconf.values[which];
	} else {
		mod_assert.ok(propconf.probdist);
		rand = Math.random();
		cmrand = 0;
		for (j = 0; j < propconf.probdist.length - 1; j++) {
			cmrand += propconf.probdist[j][0];
			if (cmrand > rand)
				break;
		}

		pconf = propconf.probdist[j];
		value = Math.round(
		    Math.random() * (pconf[2] - pconf[1]) + pconf[1]);
	}

	o[propconf.name] = value;
	if (propconf.dependents) {
		propconf.dependents[value].forEach(function (pc) {
			doProp(obj, pc);
		});
	}
}

main();
