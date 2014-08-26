#!/bin/bash

#
# tst.scan_file.sh: tests basic "scan" operations on files.
#

set -o errexit
. $(dirname $0)/../common.sh

function scan
{
	echo "# dn scan" "$@"
	dn scan "$@" test_file
	echo

	echo "# dn scan --points" "$@"
	dn scan --points "$@" test_file | sort -d
	echo
}

dn_clear_config
dn datasource-add test_file --path=$DN_DATADIR/2014/05-01/one.log
. $(dirname $0)/../scan_testcases.sh
dn_clear_config
