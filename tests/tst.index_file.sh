#!/bin/bash

#
# test.index_file.sh: tests index operations on files
#

set -o errexit

tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile" >&2

function scan
{
	echo "# dn query" "$@"
	dn query "$@" $tmpfile
	echo
}

# Try all the "scan" test cases with an index that should handle them all.
dn index -c host,operation,req.caller,req.method,latency[aggr=quantize] \
    $DATADIR/one.log $tmpfile
. $(dirname $0)/scan_testcases.sh

# That should have been pretty exhaustive, but try an index with a filter on it.
dn index -f '{ "eq": [ "req.method", "GET" ] }' $DATADIR/one.log $tmpfile
scan

rm -f $tmpfile
