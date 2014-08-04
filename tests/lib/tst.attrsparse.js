/*
 * tst.attrsparse.js: test attributes parser
 */

var mod_assert = require('assert');
var attrsParse = require('../../lib/attr-parser');

var testcases = [ {
    'str': 'foo',
    'parsed': [ { 'name': 'foo' } ]
}, {
    'str': 'foo,bar',
    'parsed': [ { 'name': 'foo' }, { 'name': 'bar' } ]
}, {
    'str': 'foo[b]',
    'parsed': [ { 'name': 'foo', 'b': '' } ]
}, {
    'str': 'foo[boolprop]',
    'parsed': [ { 'name': 'foo', 'boolprop': '' } ]
}, {
    'str': 'foo[myprop=one]',
    'parsed': [ { 'name': 'foo', 'myprop': 'one' } ]
}, {
    'str': 'foo[myprop=one],bar',
    'parsed': [ { 'name': 'foo', 'myprop': 'one' }, { 'name': 'bar' } ]
}, {
    'str': 'foo[p1=one,p2,p3=three],bar',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': ',foo[p1=one,p2,p3=three],bar',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': 'foo[p1=one,p2,p3=three],bar,',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': 'foo[p1=one,p2,p3=three],,bar',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': 'foo[p1=one,p2,,p3=three],,bar',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': 'foo[p1=one,p2,p3=three],bar[]',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar' }
    ]
}, {
    'str': 'foo[p1=one,p2,p3=three],bar[,p4]',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar', 'p4': '' }
    ]
}, {
    'str': 'foo[p1=one,p2,p3=three],bar[,p4=]',
    'parsed': [
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'bar', 'p4': '' }
    ]
}, {
    'str': 'bar,foo[p1=one,p2,p3=three],baz,qant[p1=onetwo],junk[p5]',
    'parsed': [
	{ 'name': 'bar' },
	{ 'name': 'foo', 'p1': 'one', 'p2': '', 'p3': 'three' },
	{ 'name': 'baz' },
	{ 'name': 'qant', 'p1': 'onetwo' },
	{ 'name': 'junk', 'p5': '' }
    ]
} ];
var errors = [ {
    'str': 'foo[',
    'message': 'unexpected end of string'
}, {
    'str': 'foo[foo',
    'message': 'unexpected end of string'
}, {
    'str': 'foo[foo=',
    'message': 'unexpected end of string'
}, {
    'str': 'foo[=]',
    'message': 'missing attribute name'
}, {
    'str': 'foo[=bar]',
    'message': 'missing attribute name'
}, {
    'str': 'foo,[]',
    'message': 'missing field name'
}, {
    'str': 'foo,[bar=baz]',
    'message': 'missing field name'
} ];

testcases.forEach(function (testcase, i) {
	var result;
	console.error('test case %d: "%s"', i, testcase.str);
	result = attrsParse(testcase.str);
	console.error(result);
	mod_assert.deepEqual(result, testcase.parsed);
});

errors.forEach(function (testcase, i) {
	var result;
	console.error('error case %d: "%s"', i, testcase.str);
	result = attrsParse(testcase.str);
	console.error(result);
	mod_assert.ok(result instanceof Error);
	mod_assert.equal(result.message, testcase.message);
});
