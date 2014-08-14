function dn_fail
{
	echo "failed" >&2
	exit 1
}

function dn_clear_config
{
	rm -f $DRAGNET_CONFIG
}

__dir="$(dirname $BASH_SOURCE[0])/../.."

cd "$__dir" || dn_fail
__dir="$PWD"
cd - > /dev/null 2>&1 || dn_fail

export PATH=$PATH:$__dir/bin
export DN_DATADIR=$__dir/tests/data
export DRAGNET_CONFIG=/var/tmp/dragnet_test_config.json

trap dn_clear_config EXIT
