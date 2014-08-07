#!/bin/bash

#
# tst.config.sh: tests data source configuration
#

tmpfile=/var/tmp/$(basename $0).$$
echo "using tmpfile $tmpfile" >&2

md5cmd=""
expmd5=""

function rundn
{
	echo "# dn" "$@"
	DRAGNET_CONFIG=$tmpfile dn "$@"
	echo
}

if [[ -f $HOME/.dragnetrc ]]; then
	md5cmd="$(which md5)"
	[[ ! -f "$md5cmd" ]] && md5cmd="$(which md5sum)"
	expmd5="$($md5cmd < $HOME/.dragnetrc)"
fi

set -o errexit
rundn datasource-list
rundn datasource-list -v

rundn datasource-add junk --path=/junk
rundn datasource-list
rundn datasource-list -v

rundn datasource-remove junk
rundn datasource-list
rundn datasource-list -v

rundn datasource-add manta-based --backend=manta --path=/junk
rundn datasource-list
rundn datasource-list -v

rundn datasource-add manta-based2 --backend=manta --path=/junk \
    --time-format=%Y/%m/%d/%H --data-format=json-skinner
rundn datasource-list
rundn datasource-list -v

rundn datasource-remove manta-based2
rundn datasource-list
rundn datasource-list -v

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
