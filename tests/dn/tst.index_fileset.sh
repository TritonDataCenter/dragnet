#!/bin/bash

#
# test.index_fileset.sh: tests index operations on a small fileset.
#

set -o errexit
. $(dirname $0)/common.sh

tmpdir="/var/tmp/$(basename $0).$$"
echo "using tmpdir \"$tmpdir" >&2

function scan
{
	echo "# dn query" "$@"
	dn query -I $tmpdir "$@"
	echo
}

# Try all the "scan" test cases with an index that should handle them all.
echo "creating indexes" >&2
dn index -c host,operation,req.caller,req.method,latency[aggr=quantize] \
    -R $DN_DATADIR -I "$tmpdir"
(cd "$tmpdir" && find . -type f | sort -n)
. $(dirname $0)/scan_testcases.sh

# That should have been pretty exhaustive, but try an index with a filter on it.
echo "creating filtered index" >&2
dn index -f '{ "eq": [ "req.method", "GET" ] }' -R $DN_DATADIR -I "$tmpdir"
scan

rm -rf "$tmpdir"
