#!/bin/bash
#
# tst.badargs.sh: test miscellaneous bad arguments
#

. $(dirname $0)/../common.sh

file=$DN_DATADIR/2014/05-01/one.log
function try
{
	if dn scan-file "$@" $file 2>&1; then
		echo "unexpected success (args: $@)"
		exit 1
	fi

	return 0
}

try -b host -b req.method,x[=bar]
try -b host -b req.method,[]
try -b host -b req.method,foo[
try -f '{'
try -f '{ "junk": [ "foo", "bar" ] }'
try --data-format=junk
