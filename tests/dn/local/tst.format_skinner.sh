#!/bin/bash

#
# tst.format_skinner.sh: tests "scan", "index", and "query" operations on
# skinner-style points as input data.
#

set -o errexit
. $(dirname $0)/../common.sh

function trace
{
	echo "#" "$@"
	"$@"
}

tmpfile="/var/tmp/$(basename $0).$$"
tmpfile2="$tmpfile.2"
echo "using tmpfiles \"$tmpfile\" and \"$tmpfile2\"" >&2

# Test points with no fields
dn scan-file --points $DN_DATADIR/2014/05-01/one.log > $tmpfile
cat $tmpfile | trace dn scan-file --data-format=json-skinner /dev/stdin
cat $tmpfile | trace dn scan-file --data-format=json-skinner /dev/stdin
cat $tmpfile $tmpfile | trace dn scan-file --data-format=json-skinner /dev/stdin
cat $tmpfile $tmpfile $tmpfile | \
    trace dn scan-file --data-format=json-skinner /dev/stdin

# Test points with a couple of fields
dn scan-file --points -b req.method,res.statusCode \
    $DN_DATADIR/2014/05-01/one.log > $tmpfile
dn scan-file -b req.method $DN_DATADIR/2014/05-01/one.log
cat $tmpfile $tmpfile $tmpfile | \
    trace dn scan-file --data-format=json-skinner /dev/stdin
cat $tmpfile $tmpfile $tmpfile | \
    trace dn scan-file -b req.method --data-format=json-skinner /dev/stdin

# Test indexes
echo "building index"
cat $tmpfile $tmpfile $tmpfile | \
    dn index-file -c req.method --data-format=json-skinner /dev/stdin $tmpfile2
dn query-file $tmpfile2
dn query-file -b req.method $tmpfile2

rm -f $tmpfile $tmpfile2
