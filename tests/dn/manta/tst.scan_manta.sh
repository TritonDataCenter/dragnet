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
	echo "# dn scan-manta" "$@"
	dn scan-manta "$@" $DN_MANTADIR
	echo

	echo "# dn scan-manta --points" "$@"
	dn scan-manta --points "$@" $DN_MANTADIR | sort -d
	echo
}

if [[ -z "$MANTA_URL" || -z "$MANTA_USER" ]]; then
	echo "error: This test requires the MANTA_URL and MANTA_USER" \
	    "environment variables." >&2
	exit 2
fi

. $(dirname $0)/../scan_testcases.sh

#
# The "before" and "after" filters should prune the number of objects scanned.
# When comparing output, it's important to verify the correct number of inputs
# submitted.
#
scan --counters -b 'timestamp[date,field=time,aggr=lquantize,step=86400]' \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
scan --counters --time-format=%Y/%m-%d --time-field=time \
    --after 2014-05-02 --before 2014-05-03 \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
scan --counters --time-format=%Y/%m-%d \
    -b 'timestamp[date,field=time,aggr=lquantize,step=60]' \
    --after "2014-05-02T04:05:06.123" --before "2014-05-02T04:15:10" \
    2>$tmpfile ; grep -w inputs $tmpfile; cat $tmpfile >&2
rm -f $tmpfile
