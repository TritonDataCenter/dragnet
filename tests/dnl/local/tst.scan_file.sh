#!/bin/bash

#
# tst.scan_file.sh: tests basic "scan" operations on files.
#

set -o errexit
. $(dirname $0)/../common.sh

function scan
{
	echo "# dnl scan-file" "$@"
	dnl scan-file "$@" $DN_DATADIR/2014/05-01/one.log
	echo

	echo "# dnl scan-file --points" "$@"
	dnl scan-file --points "$@" $DN_DATADIR/2014/05-01/one.log | sort -d
	echo
}

. $(dirname $0)/../scan_testcases.sh
