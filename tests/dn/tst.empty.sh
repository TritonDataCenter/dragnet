#!/bin/bash

#
# tst.empty.sh: tests basic "scan" operations on empty files
#

set -o errexit
. $(dirname $0)/common.sh

tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile\"" >&2

function scan-file
{
	echo "# dn scan-file" "$@"
	dn scan-file "$@" /dev/null 2>&1
	echo

	echo "# dn scan-file --points" "$@"
	dn scan-file --points "$@" /dev/null 2>&1 | sort -d
	echo
}

function query
{
	echo "# dn query-file" "$@"
	dn query-file "$@" $tmpfile 2>&1
}

scan-file --counters
scan-file -b timestamp
scan-file -b timestamp[aggr=quantize]
scan-file -b timestamp[aggr=quantize],req.method
scan-file -f '{ "eq": [ "audit", true ] }' -b timestamp[aggr=quantize],req.method
scan-file --counters -f '{ "eq": [ "audit", true ] }'

echo "creating index" >&2
dn index-file /dev/null $tmpfile
query --counters

echo "creating index" >&2
dn index-file -c req.method,latency[aggr=quantize] /dev/null $tmpfile
query --counters
query -f '{ "eq": [ "req.method", "GET" ] }'
query -b req.method
query -b latency
query --counters -b latency
