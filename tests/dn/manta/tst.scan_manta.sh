#!/bin/bash

#
# tst.scan_manta.sh: tests basic "scan" operations on a Manta-based dataset.
#

set -o errexit
. $(dirname $0)/../common.sh

DN_MANTADIR=/dap/public/dragnet/testdata
tmpfile="/var/tmp/$(basename $0).$$"
echo "using tmpfile \"$tmpfile\"" >&2

function scan
{
	echo "# dn scan" "$@"
	dn scan "$@" testdata
	echo

	echo "# dn scan --points" "$@"
	dn scan --points "$@" testdata | sort -d
	echo
}

if [[ -z "$MANTA_URL" || -z "$MANTA_USER" ]]; then
	echo "error: This test requires the MANTA_URL and MANTA_USER" \
	    "environment variables." >&2
	exit 2
fi

dn datasource-add --backend=manta --path=$DN_MANTADIR --time-field=time \
    --time-format=%Y/%m-%d testdata
. $(dirname $0)/../scan_testcases.sh

#
# The "before" and "after" filters should prune the number of objects scanned.
# When comparing output, it's important to verify the correct number of inputs
# submitted.
#
scan -n -b 'timestamp[date,field=time,aggr=lquantize,step=86400]' 2>&1
scan --counters -b 'timestamp[date,field=time,aggr=lquantize,step=86400]' \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
scan -n --after 2014-05-02 --before 2014-05-03 2>&1
scan --counters --after 2014-05-02 --before 2014-05-03 \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
scan -n -b 'timestamp[date,field=time,aggr=lquantize,step=60]' \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" 2>&1
scan --counters -b 'timestamp[date,field=time,aggr=lquantize,step=60]' \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
rm -f $tmpfile
