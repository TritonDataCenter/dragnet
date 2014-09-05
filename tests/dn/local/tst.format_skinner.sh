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

dn_clear_config
dn datasource-add stdin --path=/dev/stdin
dn datasource-add stdin-skinner --path=/dev/stdin --data-format=json-skinner

# Test points with no fields
dn scan --points stdin < $DN_DATADIR/2014/05-01/one.log > $tmpfile

cat $tmpfile | trace dn scan stdin-skinner
cat $tmpfile $tmpfile | trace dn scan stdin-skinner
cat $tmpfile $tmpfile $tmpfile | trace dn scan stdin-skinner

# Test points with a couple of fields
dn scan --points -b req.method,res.statusCode stdin \
    < $DN_DATADIR/2014/05-01/one.log > $tmpfile
dn scan -b req.method stdin < $DN_DATADIR/2014/05-01/one.log
cat $tmpfile $tmpfile $tmpfile | trace dn scan stdin-skinner
cat $tmpfile $tmpfile $tmpfile | trace dn scan stdin-skinner -b req.method 

# Test indexes
echo "building index"
cat $tmpfile $tmpfile $tmpfile > $tmpfile2
mv $tmpfile2 $tmpfile
dn datasource-add test_input --path=$tmpfile --data-format=json-skinner \
    --index-path=$tmpfile2 
dn metric-add test_input total
dn metric-add test_input -b req.method by_method
dn build --interval=all test_input
dn query --interval=all test_input
dn query --interval=all test_input -b req.method
rm -rf $tmpfile $tmpfile2
