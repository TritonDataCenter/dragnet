#!/bin/bash

#
# test.index_fileset.sh: tests index operations on a small fileset.
#

set -o errexit
. $(dirname $0)/../common.sh

tmpdir="/var/tmp/$(basename $0).$$"
echo "using tmpdir \"$tmpdir" >&2

function scan
{
	echo "# dn query" "$@"
	dn query --interval=hour "$@" input
	echo
}

#
# Build an index that should handle the standard "scan" test cases and then try
# them out with an implementation of "scan" that queries the index.
#
dn_clear_config
dn datasource-add input --path=$DN_DATADIR --index-path=$tmpdir \
    --time-field=time
dn metric-add --datasource=input  myindex \
    -b timestamp[date,field=time,aggr=lquantize,step=86400],host,operation \
    -b req.caller,req.method,latency[aggr=quantize]
dn build --interval=hour input
(cd "$tmpdir" && find . -type f | sort -n)
. $(dirname $0)/../scan_testcases.sh
rm -rf "$tmpdir"

#
# That should have been pretty exhaustive, but try an index with a filter on it.
#
dn metric-remove myindex
dn metric-add --datasource=input --filter='{ "eq": [ "req.method", "GET" ] }' \
    -b timestamp[date,field=time,aggr=lquantize,step=86400] myindex
dn build --interval=hour input
scan -f '{ "eq": [ "req.method", "GET" ] }'
rm -rf "$tmpdir"

#
# The "before" and "after" filters should prune the number of files scanned.
# When comparing output, it's important to verify the correct number of records
# returned as well as the expected number of files scanned.
#
dn metric-remove myindex
dn metric-add --datasource=input myindex \
    -b timestamp[date,field=time,aggr=lquantize,step=60]
dn build --interval=hour input

scan --counters -b timestamp[aggr=lquantize,step=86400] 2>&1
scan --counters --after 2014-05-02 --before 2014-05-03 2>&1
scan --counters -b timestamp[aggr=lquantize,step=60] \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" 2>&1
rm -rf "$tmpdir"

dn_clear_config
dn datasource-add input --path=/dev/null --index-path=$tmpdir --time-field=time
dn metric-add --datasource=input -b timestamp[date,field=time] myindex
dn build input
if [[ -d "$tmpdir" ]]; then
	echo "FAIL: unexpectedly created $tmpdir" >&2
	exit 1
fi

dn_clear_config
