#!/bin/bash

#
# tst.index_manta.sh: tests index and query operations on a Manta-based dataset.
#

set -o errexit
. $(dirname $0)/common.sh

DN_MANTADIR=/dap/public/dragnet/testdata
tmpdir="/dap/stor/$(basename $0).$$"
echo "using Manta tmpdir \"$tmpdir" >&2

function scan
{
	echo "# dn query-manta" "$@"
	dn query-mjob "$@" "$tmpdir"
	echo
}

# Try all the "scan" test cases with an index that should handle them all.
echo "creating indexes" >&2
dn index-manta -c 'timestamp[date;field=time;aggr=lquantize;step=86400]' \
    -c host,operation,req.caller,req.method,latency[aggr=quantize] \
    --interval=day \
    $DN_MANTADIR "$tmpdir"
mfind "$tmpdir"
. $(dirname $0)/scan_testcases.sh
mrm -r "$tmpdir"

# That should have been pretty exhaustive, but try an index with a filter on it.
echo "creating filtered index" >&2
dn index-manta -f '{ "eq": [ "req.method", "GET" ] }' \
    -c 'timestamp[date;field=time;aggr=lquantize;step=86400]' \
    $DN_MANTADIR "$tmpdir"
scan
mrm -r "$tmpdir"
