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
	echo "# dn query-tree" "$@"
	dn query-tree "$@" $tmpdir
	echo
}

# Try all the "scan" test cases with an index that should handle them all.
echo "creating indexes" >&2
dn index-tree -c 'timestamp[date,field=time,aggr=lquantize,step=86400],host,operation,req.caller,req.method,latency[aggr=quantize]' \
    $DN_DATADIR "$tmpdir"
(cd "$tmpdir" && find . -type f | sort -n)
. $(dirname $0)/scan_testcases.sh
rm -rf "$tmpdir"

# That should have been pretty exhaustive, but try an index with a filter on it.
echo "creating filtered index" >&2
dn index-tree -f '{ "eq": [ "req.method", "GET" ] }' \
    -c 'timestamp[date,field=time,aggr=lquantize,step=86400]' \
    $DN_DATADIR "$tmpdir"
scan
rm -rf "$tmpdir"

#
# The "before" and "after" filters should prune the number of files scanned.
# When comparing output, it's important to verify the correct number of records
# returned as well as the expected number of files scanned.
#
echo "creating timestamp index" >&2
dn index-tree -c timestamp[date,field=time,aggr=lquantize,step=60] \
    $DN_DATADIR "$tmpdir"
scan --counters -b 'timestamp[aggr=lquantize,step=86400]' 2>&1
scan --counters --time-field=timestamp \
    --after 2014-05-02 --before 2014-05-03 2>&1
scan --counters --time-field=timestamp \
    -b 'timestamp[aggr=lquantize,step=60]' \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" 2>&1
rm -rf "$tmpdir"

dn index-tree -c 'timestamp[date,field=time]' /dev/null "$tmpdir"
if [[ -d "$tmpdir" ]]; then
	echo "FAIL: unexpectedly created $tmpdir" >&2
	exit 1
fi
