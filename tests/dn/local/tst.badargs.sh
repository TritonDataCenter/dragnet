#!/bin/bash
#
# tst.badargs.sh: test miscellaneous bad arguments
#

. $(dirname $0)/../common.sh

file=$DN_DATADIR/2014/05-01/one.log
function try
{
	if dn scan "$@" input 2>&1; then
		echo "unexpected success (args: $@)"
		exit 1
	fi

	return 0
}

dn_clear_config
dn datasource-add --path=$file input
try -b host -b req.method,x[=bar]
try -b host -b req.method,[]
try -b host -b req.method,foo[
try -f '{'
try -f '{ "junk": [ "foo", "bar" ] }'

dn datasource-remove input
dn datasource-add --path=$file --data-format=junk input
try
dn_clear_config
