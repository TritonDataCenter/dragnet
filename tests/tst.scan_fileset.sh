#!/bin/bash

#
# tst.scan_fileset.sh: tests basic "scan" operations on a small fileset.
#

set -o errexit

function scan
{
	echo "# dn scan" "$@"
	dn scan -R $DATADIR "$@"
	echo

	echo "# dn scan --points" "$@"
	dn scan -R $DATADIR --points "$@" | sort
	echo
}

. $(dirname $0)/scan_testcases.sh
