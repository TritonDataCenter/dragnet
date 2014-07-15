#!/bin/bash

#
# tst.empty.sh: tests basic "scan" operations on empty files
#

set -o errexit
. $(dirname $0)/common.sh

tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile\"" >&2

function scan
{
	echo "# dn scan" "$@"
	dn scan "$@" /dev/null 2>&1
	echo

	echo "# dn scan --points" "$@"
	dn scan --points "$@" /dev/null 2>&1 | sort -d
	echo
}

function query
{
	echo "# dn query" "$@"
	dn query "$@" $tmpfile 2>&1
}

scan --counters
scan -b timestamp
scan -b timestamp[aggr=quantize]
scan -b timestamp[aggr=quantize],req.method
scan -f '{ "eq": [ "audit", true ] }' -b timestamp[aggr=quantize],req.method
scan --counters -f '{ "eq": [ "audit", true ] }'

echo "creating index" >&2
dn index /dev/null $tmpfile
query --counters

echo "creating index" >&2
dn index -c req.method,latency[aggr=quantize] /dev/null $tmpfile
query --counters
query -f '{ "eq": [ "req.method", "GET" ] }'
query -b req.method
query -b latency
query --counters -b latency
