#
# tst.scan_250k.sh: tests memory usage scanning 250K records
#

# location of tools, relative to this test file
tst_toolsdir="$(dirname $0)/../tools"

# number of records to process
tst_nrecords=250000

# maximum allowed RSS and VSZ values.
tst_maxrss=90000
tst_maxvsz=160000

# pid of process executed
tst_pid=

$tst_toolsdir/mktestdata.js $tst_nrecords | dn scan /dev/stdin &
tst_pid=$!
set -- $($tst_toolsdir/memwatch $tst_pid)
echo "rss=$1 vsz=$2" >&2

if [[ $1 -gt $tst_maxrss ]]; then
	echo "maximum rss exceeded (found $1, expected <= $tst_maxrss)" >&2
	exit 1
fi

if [[ $2 -gt $tst_maxvsz ]]; then
	echo "maximum vsz exceeded (found $2, expected <= $tst_maxvsz)" >&2
	exit 1
fi
