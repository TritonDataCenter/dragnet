#!/bin/bash

#
# tst.scan_file.sh: tests basic "scan" operations on files.
#

set -o errexit

function scan
{
	echo "# dn scan" "$@"
	dn scan "$@" $DATADIR/2014/05-01/one.log
	echo

	echo "# dn scan --points" "$@"
	dn scan --points "$@" $DATADIR/2014/05-01/one.log | sort -d
	echo
}

. $(dirname $0)/scan_testcases.sh
