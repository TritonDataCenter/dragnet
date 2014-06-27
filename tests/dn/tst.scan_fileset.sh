#!/bin/bash

#
# tst.scan_fileset.sh: tests basic "scan" operations on a small fileset.
#

set -o errexit
. $(dirname $0)/common.sh

function scan
{
	echo "# dn scan" "$@"
	dn scan -R $DN_DATADIR "$@"
	echo

	echo "# dn scan --points" "$@"
	dn scan -R $DN_DATADIR --points "$@" | sort -d
	echo
}

. $(dirname $0)/scan_testcases.sh
