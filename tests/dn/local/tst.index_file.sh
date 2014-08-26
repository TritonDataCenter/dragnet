#!/bin/bash

#
# test.index_file.sh: tests index operations on files
#

set -o errexit
. $(dirname $0)/../common.sh

tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile\"" >&2

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
dn datasource-add input --path=$DN_DATADIR/2014/05-01/one.log \
    --index-path=$tmpfile --time-field=time
dn metric-add --datasource=input big_metric \
    -b host,operation,req.caller,req.method,latency[aggr=quantize]
dn build input
. $(dirname $0)/../scan_testcases.sh

# That should have been pretty exhaustive, but try an index with a filter on it.
dn metric-remove big_metric
dn metric-add --datasource=input filtered_metric \
    -f '{ "eq": [ "req.method", "GET" ] }'
dn build input
scan -f '{ "eq": [ "req.method", "GET" ] }'

dn_clear_config
rm -rf $tmpfile