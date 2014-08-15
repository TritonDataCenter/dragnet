#!/bin/bash

#
# tst.index_manta.sh: tests index and query operations on a Manta-based dataset.
#

set -o errexit
. $(dirname $0)/../common.sh

DN_MANTADIR=/dap/public/dragnet/testdata
localtmpdir="/var/tmp/$(basename $0).$$"
tmpdir="/$MANTA_USER/stor/$(basename $0).$$"
echo "using Manta tmpdir \"$tmpdir" >&2

function scan
{
	echo "# dn query" "$@"
	dn query "$@" input
	echo
}

#
# Build an index that should handle the standard "scan" test cases and then try
# them out with an implementation of "scan" that queries the index.
#
dn_clear_config
dn datasource-add input --backend=manta --path=$DN_MANTADIR \
    --time-field=time --index-path="$tmpdir"
dn metric-add --datasource=input mymet \
    -b timestamp[date,field=time,aggr=lquantize,step=86400] \
    -b host,operation,req.caller,req.method,latency[aggr=quantize]
dn build input
mfind -n '.*.sqlite' -t o "$tmpdir" | cut -d/ -f5-
. $(dirname $0)/../scan_testcases.sh

#
# That should have been pretty exhaustive, but try an index with a filter on it.
#
echo "creating filtered index" >&2
dn metric-remove mymet
dn metric-add --datasource=input -f '{ "eq": [ "req.method", "GET" ] }' \
    -b timestamp[date,field=time,aggr=lquantize,step=86400] mymet
dn build input
scan -f '{ "eq": [ "req.method", "GET" ] }'
mrm -r "$tmpdir"
dn_clear_config
