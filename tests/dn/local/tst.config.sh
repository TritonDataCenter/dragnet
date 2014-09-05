#!/bin/bash

#
# tst.config.sh: tests data source configuration
#

. $(dirname $0)/../common.sh

tmpfile=/var/tmp/$(basename $0).$$
echo "using tmpfile $tmpfile" >&2

md5cmd=""
expmd5=""

function rundn
{
	echo "# dn" "$@"
	DRAGNET_CONFIG=$tmpfile dn "$@"
	status=$?
	echo
	return $status
}

function shouldfail
{
	if "$@" 2>&1 | head -3; then
		echo "didn't expect that to succeed!" >&2
		exit 1
	fi

	return 0
}

if [[ -f $HOME/.dragnetrc ]]; then
	md5cmd="$(which md5)"
	[[ ! -f "$md5cmd" ]] && md5cmd="$(which md5sum)"
	expmd5="$($md5cmd < $HOME/.dragnetrc)"
fi

set -o errexit
set -o pipefail

#
# Data sources
#

# Dump initial state
rundn datasource-list
rundn datasource-list -v

# Fail: no path
shouldfail rundn datasource-add junk3
# Fail: bad filter
shouldfail rundn datasource-add junk3 --filter='{' --path=/junk

# Basic add
rundn datasource-add junk --path=/junk
# Add with filter
rundn datasource-add junk2 --path=/junk \
    --filter='{ "eq": [ "req.method", "GET" ] }'
# Dump state
rundn datasource-list
rundn datasource-list -v
rundn datasource-show junk
rundn datasource-show -v junk

# Fail: duplicate name
shouldfail rundn datasource-add junk --path=/junk

# Update
rundn datasource-update junk2 --backend=manta --path=/foo/bar \
    --index-path=/bar/foo --filter={} --data-format=json-skinner \
    --time-format=%Y --time-field=foo
rundn datasource-show junk2
rundn datasource-show -v junk2
shouldfail rundn datasource-update
shouldfail rundn datasource-update nonexistent

# Clean up and dump state
rundn datasource-remove junk2
rundn datasource-list
rundn datasource-list -v

rundn datasource-remove junk
rundn datasource-list
rundn datasource-list -v

# Fail: datasource not present
shouldfail rundn datasource-remove junk

# Add Manta-based dataset with more parameters
rundn datasource-add manta-based --backend=manta --path=/junk
rundn datasource-add manta-based2 --backend=manta --path=/junk \
    --time-format=%Y/%m/%d/%H --data-format=json-skinner
rundn datasource-list
rundn datasource-list -v

#
# Metrics
#

# Dump initial state
rundn metric-list manta-based
rundn metric-list manta-based2
rundn metric-list -v manta-based
rundn metric-list -v manta-based2

# Fail: bad filter
shouldfail rundn metric-add --filter={ manta-based met1
# Fail: no datasource
shouldfail rundn metric-add met1

# Basic add
rundn metric-add manta-based met1
rundn metric-list manta-based
rundn metric-list -v manta-based

# Add with filter and breakdowns
rundn metric-add --filter='{ "eq": [ "req.method", "GET" ] }' manta-based met2
rundn metric-add --filter='{ "eq": [ "req.method", "GET" ] }' \
    --breakdowns=host,req.method,latency[aggr=quantize] manta-based met3
rundn metric-list manta-based
rundn metric-list -v manta-based

# Fail: duplicate name
shouldfail rundn metric-add manta-based met1

rundn metric-remove manta-based met1
rundn metric-remove manta-based met2
rundn metric-remove manta-based met3
shouldfail rundn metric-remove manta-based met2

# Clean up and dump state.
rundn datasource-remove manta-based2
rundn datasource-remove manta-based
rundn datasource-list
rundn datasource-list -v

rm -f $tmpfile

if [[ -n "$expmd5" ]]; then
	if [[ "$($md5cmd < $HOME/.dragnetrc)" != "$expmd5" ]]; then
		echo "oops! clobbered .dragnetrc!" >&2
		exit 1
	fi
fi
