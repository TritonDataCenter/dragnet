__dir="$(dirname $BASH_SOURCE[0])/../.."
export PATH=$PATH:$__dir/bin
export DN_DATADIR=$__dir/tests/data
export DRAGNET_CONFIG=/var/tmp/dragnet_test_config.json

function dn_clear_config
{
	rm -f $DRAGNET_CONFIG
}

trap dn_clear_config EXIT
