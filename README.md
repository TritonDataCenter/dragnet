# Dragnet

"dn" is a tool for analyzing event stream data stored in files.  There are three
commands:

* "scan": scan over raw data to execute a query
* "index": scan over raw data to produce an index
* "query": search indexes to execute a query

The point is to "index" data and then "query" it.  "scan" is available to check
results and to run ad-hoc queries that it may not make sense to index.

## Reference

If you don't already know what "dn" does, you're better off starting with the
"Getting Started" section below.

### Scanning raw data

General form:

    dn scan   [-b|--breakdowns COLUMN[,COLUMN...]]
              [-f|--filter FILTER]
              [--before END_TIMESTAMP] [--after START_TIMESTAMP]
              [--time-format=TIME_FORMAT] [--time-field=FIELDNAME]
              [--data-format json|json-skinner]
              [--counters] [--points] [--show-warnings]
              DATA_FILE | -R dataroot'

Scan the records in a single newline-separated-JSON data file:

    dn scan SCAN_OPTIONS data_file

Scan the records in all files in "data\_directory":

    dn scan SCAN_OPTIONS -R data_directory

Scan only data from the first few days of July, assuming data is laid out under
"data\_directory/YYYY/MM/DD":

    dn scan SCAN_OPTIONS -R data_directory --time-format=%Y/%m/%d
        --after 2014-07-01 --before 2014-07-04

SCAN\_OPTIONS include:

* `-b | --breakdowns COLUMN[,COLUMN...]`: A list of column definitions by which
  to break out the results.  With no breakdowns specified, the result of a scan
  is a count of all records scanned (excluding those dropped by the filter).
  With a breakdown on a column like "req.method" (request method, which is
  usually a string like "GET" or "PUT"), the result is a count for each value of
  "req.method" that was found.  With a breakdown on two columns, the result is a
  count for each unique combination of values for those columns (e.g., 15
  records with "req.method" equal to "GET" and "res.statusCode" equal to "200").
  To avoid exploding the number of results, you can group nearby values of
  numeric quantities using an aggregation.  See "Getting started" below for
  details.
* `-f | --filter FILTER`: A node-krill (JSON format) predicate to evaluate on
  each record.  Records not matching the filter, as well as records missing
  fields that are used by the filter, are dropped.
* `--before END_TIMESTAMP`: Only scan data files containing data before
  END\_TIMESTAMP, and filter out data points after END\_TIMESTAMP (exclusive).
  Requires you to specify `--time-format`.
* `--after START_TIMESTAMP`: Only scan data files containing data after
  START\_TIMESTAMP, and filter out data points before START\_TIMESTAMP
  (inclusive).  Requires you to specify `--time-format`.
* `--time-format TIME_FORMAT`: Specifies how the names of directories and files
  under "data\_directory" correspond with the timestamps of the data points
  contained in each file.  This is a format string like what strftime(3C)
  supports, except that only "%Y", "%m", "%d", and "%H" are currently
  implemented.  This is used to prune data that has to be scanned when using
  --before and --after.
* `--time-field TIME_FIELD`: Specifies which field contains the timestamp.  This
  is used for --before and --after.
* `--data-format json | json-skinner`: Specifies the incoming data format.
  Currently, only newline-separated JSON data ("json") and an internal
  node-skinner format ("json-skinner") are supported.

There are a few debugging options:

* `--counters`: upon completion, show non-zero values of miscellaneous internal
  counters, which include things like inputs processed at each state of the
  pipeline, records filtered out, records with invalid fields, and so on.  The
  names of internal streams, their counters, and the output format are not
  stable and are subject to change at any time.
* `--points`: emit data as node-skinner data points rather than human-readable
  results.  node-skinner points are similar to the input data except that they
  include a "value" field for representing N instances of the same record
  without replicating the record N times.  These points can be used as input to
  subsequent scans or indexes using --data-format=json-skinner.
* `--show-warnings`: as data is scanned, show warnings about records that are
  dropped.  Common reasons include: filtered out by a --filter filter, filtered
  out by --before or --after, failed to evaluate the --filter (e.g., because a
  field specified in the filter isn't present), failed to parse a numeric field
  (e.g., a field with "aggr"), or failed to parse a timestamp field.  As with
  --counters, everything about this option's output is unstable and subject to
  change at any time.

### Indexing

General form:

    dn index  [-c|--columns COLUMN[,COLUMN...]]
              [-f|--filter FILTER]
              [--before END_TIMESTAMP] [--after START_TIMESTAMP]
              [--time-format=TIME_FORMAT]
              [--data-format json|json-skinner]
              [-i|--interval hour|day]
              [-s|--source hour]
              [--counters] [--show-warnings]
              DATA_FILE INDEX_FILE | -R dataroot [-I dataroot]

Generate a single index file from a single newline-separated-JSON data file:

    dn index INDEX_OPTIONS data_file index_file

Generate hourly index files into "index\_directory" from data stored in
"data\_directory":

    dn index INDEX_OPTIONS -R data_directory [-I index_directory]

Generate daily index files instead:

    dn index INDEX_OPTIONS --interval=day -R data_directory [-I index_directory]

Generate hourly indexes, but only for the first few days of July, assuming data
is laid out under "data\_directory/YYYY/MM/DD"

    dn index INDEX_OPTIONS -R data_directory [-I index_directory] \
        --time-format=%Y/%m/%d --after 2014-07-01 --before 2014-07-04

INDEX\_OPTIONS include:

* `-c | --columns COLUMN[,COLUMN]`: Same as columns for "dn scan --breakdowns".
* `-f | --filter FILTER`: Same as "dn scan --filter".
* `--after START_TIMESTAMP`: Same as "dn scan --after".
* `--before END_TIMESTAMP`: Same as "dn scan --before".
* `--time-format TIME_FORMAT`: Same as "dn scan --time-format".
* `--data-format json | json-skinner`: Same as "dn scan --data-format".
* `-i | --interval INTERVAL`: Specifies that indexes should be chunked into
  files by INTERVAL, which is either "hour" or "day".  This is only supported
  when -R is used.  The default is "hour".
* `-s | --source hour`: Specifies that the underlying data for the index
  should come from hourly indexes instead of the raw data files, which is useful
  to build daily indexes more efficiently.

To specify the time resolution of each index file, you specify your own
"timestamp" column.  For example, specifying column
`timestamp[date;field=time;aggr=lquantize;step=60]` adds a field called
"timestamp" to the index which is the result of parsing the "time" field in the
raw data as an ISO 8601 timestamp and converting that to a Unix timestamp
(seconds since the epoch).  The result is bucketed by minute (`step=60`).  If
you want the resolution to be 10 seconds instead, use `step=10`.

There are a few debugging options:

* `--counters`: See "dn scan --counters".
* `--show-warnings`: See "dn scan --show-warnings".

When using forms of "dn index" that generate multiple index files (e.g., hourly
or daily) and the source is raw data (rather than another index), you must
include at least one column that's a "date" field.  That field will be used to
figure out which hourly or daily index file a given data point should wind up
in.


### Querying

"dn query" supports arguments like "dn scan":

    dn query  [-b|--breakdowns COLUMN[,COLUMN...]]
              [-f|--filter FILTER]
              [--before END_TIMESTAMP] [--after START_TIMESTAMP]
              [--time-field TIME_FIELD]
              [--counters]
              INDEX_FILE | -I indexroot

All of these options function as documented for "dn scan".  If you specify
"INDEX\_FILE", that file should be a single index file to be queried.  If you
specify -I instead, "indexroot" refers to a directory of indexes created with
"dn index".  "dn" will automatically select the daily indexes if available and
fall back to hourly indexes if not.

Several "dn scan" arguments are not supported by "dn query" because they don't
apply:

* `--data-format` doesn't apply because the format of indexes is fixed.
* `--time-format` doesn't apply because the structure of the index directory
  tree is fixed.
* `--show-warnings` doesn't apply because any problems parsing indexes is
  considered a fatal error.

The fact that --time-field is ever necessary for "dn query" is a bug.


## Getting started

dragnet only supports newline-separated JSON.  Try it on the sample data in
tests/data.  In the simplest mode, dragnet operates on raw files.  With no
arguments, "scan" just counts records:

    $ dn scan 08-02d02889.log 
    VALUE
     1000

You can also break out results, e.g., by request method:

    $ dn scan -b req.method 08-02d02889.log 
    REQ.METHOD VALUE
    DELETE         1
    GET          300
    HEAD         402
    POST           4
    PUT          191
    undefined    102

"undefined" shows up here because there are events with no request method.
We'll see this in several of the examples because my example log has some other
kinds of events in it.

You can break out results by more than one field:

    $ dn scan -b req.method,res.statusCode 08-02d02889.log 
    REQ.METHOD RES.STATUSCODE VALUE
    DELETE     204                1
    GET        200              286
    GET        302                1
    GET        403                1
    GET        404               12
    HEAD       200              313
    HEAD       404               89
    POST       201                3
    POST       202                1
    PUT        204              189
    PUT        403                1
    PUT        404                1
    undefined  undefined        102

and the order matters.  If we reverse these:

    $ dn scan -b res.statusCode,req.method 08-02d02889.log
    RES.STATUSCODE REQ.METHOD VALUE
    200            GET          286
    200            HEAD         313
    201            POST           3
    202            POST           1
    204            DELETE         1
    204            PUT          189
    302            GET            1
    403            GET            1
    403            PUT            1
    404            GET           12
    404            HEAD          89
    404            PUT            1
    undefined      undefined    102

You can also filter records out using
[node-krill](https://github.com/joyent/node-krill) filter syntax:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' 08-02d02889.log 
    VALUE
      300

You can combine this with breakdowns, of course:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' -b operation 08-02d02889.log 
    OPERATION        VALUE
    get100               1
    getjobstatus        30
    getpublicstorage   126
    getstorage         143

To break down by numeric quantities, it's usually best to aggregate nearby
values into buckets.  Here's a histogram of the "latency" field from this log:

    $ dn scan -b latency[aggr=quantize] 08-02d02889.log 

               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         1
                   2 |                                         1
                   4 |@                                        21
                   8 |@@@@@                                    118
                  16 |@@@@@@@@@@@@@@@@@@@@@@@                  585
                  32 |@@@@@@                                   152
                  64 |@                                        35
                 128 |@@                                       59
                 256 |@                                        20
                 512 |                                         8
                1024 |                                         0

"aggr=quantize" denotes a power-of-two bucketization.  You can also do a linear
quantization with steps of size 50 (notice the escaped semicolon):

    $ dn scan -b latency[aggr=lquantize\;step=50] 08-02d02889.log 

               value  ------------- Distribution ------------- count
                   0 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@      865
                  50 |@                                        35
                 100 |@@                                       39
                 150 |@                                        22
                 200 |                                         10
                 250 |                                         7
                 300 |                                         6
                 350 |                                         2
                 400 |                                         3
                 450 |                                         1
                 500 |                                         3
                 550 |                                         2
                 600 |                                         2
                 650 |                                         2
                 700 |                                         0
                 750 |                                         1
                 800 |                                         0

These are modeled after DTrace's aggregating actions.  You can combine these
with other breakdowns:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' \
          -b req.method,operation,latency[aggr=quantize] 08-02d02889.log 

    GET, getpublicstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |                                         0
                   4 |@@@@                                     12
                   8 |@@@@@@@@@@@@@@@@@                        52
                  16 |@@@@@@@@@@@@@@@@@                        54
                  32 |@@                                       6
                  64 |@                                        2
                 128 |                                         0

    GET, getstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |                                         0
                   4 |                                         1
                   8 |@@@                                      10
                  16 |@@@@@@@@@@@@@@@@@@@@                     71
                  32 |@@@@@@@@@@@@@@@@                         57
                  64 |@                                        3
                 128 |                                         1
                 256 |                                         0

    GET, getjobstatus
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |                                         0
                   4 |@                                        1
                   8 |@@@@@@@@                                 6
                  16 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@          23
                  32 |                                         0

    GET, get100
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 1
                   2 |                                         0

If the last field isn't an aggregation, "dn" won't print a histogram, but it
will still group nearby values.  For example, if we reverse the order of that
last one:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' \
          -b latency[aggr=quantize],req.method,operation 08-02d02889.log 
    LATENCY REQ.METHOD OPERATION        VALUE
          1 GET        get100               1
          4 GET        getjobstatus         1
          4 GET        getpublicstorage    12
          4 GET        getstorage           1
          8 GET        getjobstatus         6
          8 GET        getpublicstorage    52
          8 GET        getstorage          10
         16 GET        getjobstatus        23
         16 GET        getpublicstorage    54
         16 GET        getstorage          71
         32 GET        getpublicstorage     6
         32 GET        getstorage          57
         64 GET        getpublicstorage     2
         64 GET        getstorage           3
        128 GET        getstorage           1

You can get per-interval results by specifying one of the fields as a "date"
parsed from some other field and then aggregating on it.  The value of such a
field is a Unix timestamp.  For an example, here's how you can get per-hour data
from a 3-hour log file, where the timestamp is stored in a field called "time":

    $ dn scan -b 'timestamp[date;field=time;aggr=lquantize;step=3600],req.method' mydata.log
     TIMESTAMP REQ.METHOD VALUE
    1401570000 DELETE        82
    1401570000 GET           84
    1401570000 HEAD          86
    1401570000 PUT           82
    1401573600 DELETE        94
    1401573600 GET           59
    1401573600 HEAD          96
    1401573600 PUT           84
    1401577200 DELETE        83
    1401577200 GET           81
    1401577200 HEAD          95
    1401577200 PUT           74


## Indexes

All of the examples above used a full file scan just to demonstrate the data
model.  The point of Dragnet is to create indexes that can answer these same
queries much faster.  You can index a file much the way you write a query.
Here's an example that creates indexes on the request method, operation, and
latency:

    $ dn index -c req.method,operation,latency[aggr=quantize] 08-02d02889.log myindex
    index "myindex" created

My sample data is fairly small, but the index is much smaller:

    $ ls -lh 08-02d02889.log myindex 
    -rw-r--r-- 1 dap other 2.1M Jun 19 12:42 08-02d02889.log
    -rw-r--r-- 1 dap other 8.0K Jun 20 16:41 myindex

Indexes are currently just sqlite databases with the same results that a
similar query would have:

    $ sqlite3 myindex '.schema dragnet_index'
    CREATE TABLE dragnet_index(
        req_method varchar(128),
        operation varchar(128),
        latency integer,
        value integer
    );

    $ sqlite3 myindex 'select * from dragnet_index'
    GET|getpublicstorage|4|12
    GET|getpublicstorage|8|52
    GET|getpublicstorage|16|54
    GET|getpublicstorage|32|6
    GET|getpublicstorage|64|2
    GET|getstorage|4|1
    GET|getstorage|8|10
    GET|getstorage|16|71
    ...

You can query an index with the same syntax you'd use for "scan", but with the
"query" command:

    $ dn query -f '{ "eq": [ "req.method", "GET" ] }' \
          -b latency[aggr=quantize],req.method,operation myindex 
    LATENCY REQ.METHOD OPERATION        VALUE
          1 GET        get100               1
          4 GET        getjobstatus         1
          4 GET        getpublicstorage    12
          4 GET        getstorage           1
          8 GET        getjobstatus         6
          8 GET        getpublicstorage    52
          8 GET        getstorage          10
         16 GET        getjobstatus        23
         16 GET        getpublicstorage    54
         16 GET        getstorage          71
         32 GET        getpublicstorage     6
         32 GET        getstorage          57
         64 GET        getpublicstorage     2
         64 GET        getstorage           3
        128 GET        getstorage           1

"query" and "scan" should return the same results -- the point is that "query"
should be much faster.


## Beyond files

All of the examples used a single data file and a single index file to
demonstrate the main ideas, but Dragnet is designed for larger corpuses with
many files.  Your data set can have as many files as you want, and Dragnet
creates per-hour index files by default using the "time" field in each JSON
object.

Here's a directory with two files, each containing three hours' worth of random
data:

    $ find data/ -type f
    data/one.log
    data/two.log

You can scan the entire directory tree by specifying it with -R (instead of
specifying a file name):

    $ dn scan -R data
    VALUE
     2000

    $ dn scan -b req.method -R data
    REQ.METHOD VALUE
    DELETE       478
    GET          503
    HEAD         513
    PUT          506

You can index it the same way:

    $ dn index -c req.method,res.statusCode,latency[aggr=quantize] -R data
    inferring index root: data_index

By default, "dn" puts the indexes in a parallel tree with a "\_index" suffix:

    $ find data_index/ -type f
    data_index/2014-05-31-22.sqlite
    data_index/2014-05-31-21.sqlite
    data_index/2014-05-31-23.sqlite

Notice there are three index files: one for each hour of data from the original
data set.  The number of indexes doesn't depend on the size or number of input
files.  You never need to worry about the number of index files, though.  "dn"
takes care of searching all of them (or whichever ones need to be searched).

You can query these indexes by specifying the index directory with "-I":

    $ dn query -b req.method -I data_index
    REQ.METHOD VALUE
    DELETE       478
    GET          503
    HEAD         513
    PUT          506


## A note on performance

No major performance work has been done.  There's currently a lot of startup
cost.


## Status

The main next steps are:

- Add support for scanning, indexing, and querying data stored in Manta.
- Verify that performance for indexed scans is good.

In the long run, we'll want to add support for other data formats as well.
