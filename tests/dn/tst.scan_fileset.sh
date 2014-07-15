#!/bin/bash

#
# tst.scan_fileset.sh: tests basic "scan" operations on a small fileset.
#

set -o errexit
. $(dirname $0)/common.sh

function scan
{
	echo "# dn scan-tree" "$@"
	dn scan-tree "$@" $DN_DATADIR
	echo

	echo "# dn scan-tree --points" "$@"
	dn scan-tree --points "$@" $DN_DATADIR | sort -d
	echo
}

. $(dirname $0)/scan_testcases.sh

#
# The "before" and "after" filters should prune the number of files scanned.
# When comparing output, it's important to verify the correct number of records
# returned as well as the expected number of files scanned.
#
scan --counters -b 'timestamp[date;field=time;aggr=lquantize;step=86400]' 2>&1
scan --counters --time-format=%Y/%m-%d --time-field=time \
    --after 2014-05-02 --before 2014-05-03 2>&1
scan --counters --time-format=%Y/%m-%d \
    -b 'timestamp[date;field=time;aggr=lquantize;step=60]' \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" 2>&1
