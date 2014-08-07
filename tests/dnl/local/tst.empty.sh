#!/bin/bash

#
# tst.empty.sh: tests basic "scan" operations on empty files
#

set -o errexit
. $(dirname $0)/../common.sh

tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile\"" >&2

function scan-file
{
	echo "# dnl scan-file" "$@"
	dnl scan-file "$@" /dev/null 2>&1
	echo

	echo "# dnl scan-file --points" "$@"
	dnl scan-file --points "$@" /dev/null 2>&1 | sort -d
	echo
}

function query
{
	echo "# dnl query-file" "$@"
	dnl query-file "$@" $tmpfile 2>&1
}

scan-file --counters
scan-file -b timestamp
scan-file -b timestamp[aggr=quantize]
scan-file -b timestamp[aggr=quantize],req.method
scan-file -f '{ "eq": [ "audit", true ] }' -b timestamp[aggr=quantize],req.method
scan-file --counters -f '{ "eq": [ "audit", true ] }'

echo "creating index" >&2
dnl index-file /dev/null $tmpfile
query --counters

echo "creating index" >&2
dnl index-file -c req.method,latency[aggr=quantize] /dev/null $tmpfile
query --counters
query -f '{ "eq": [ "req.method", "GET" ] }'
query -b req.method
query -b latency
query --counters -b latency
