# Dragnet

"dn" is a tool for analyzing event stream data stored in files.  There are three
main kinds of commands:

* scan: scan over *raw data* to execute a query
* index: scan over raw data to produce an index
* query: search *indexes* to execute a query

The point is to index data and then query the index.  Scan is available to check
results and to run ad-hoc queries that it may not make sense to index.

**This project is still a prototype.**  The commands and library interfaces may
change incompatibly at any time!


## Getting started

dragnet only supports newline-separated JSON.  Try it on the sample data in
./tests/data.

In the simplest mode, dragnet operates on raw files.  With no arguments,
"scan-file" just counts records:

    $ dn scan-file ./tests/data/2014/05-02/one.log 
    VALUE
      252

You can also break out counts, e.g., by request method:

    $ dn scan-file -b req.method ./tests/data/2014/05-02/one.log 
    REQ.METHOD VALUE
    DELETE        71
    GET           58
    HEAD          56
    PUT           67

You can break out results by more than one field:

    $ dn scan-file -b req.method,res.statusCode ./tests/data/2014/05-02/one.log 
    REQ.METHOD RES.STATUSCODE VALUE
    DELETE     200                6
    DELETE     204               12
    DELETE     400               12
    DELETE     404                8
    DELETE     499               12
    DELETE     500                8
    DELETE     503               13
    GET        200               12
    GET        204                7
    GET        400               11
    GET        404                6
    GET        499               10
    GET        500                7
    GET        503                5
    HEAD       200                7
    HEAD       204                5
    HEAD       400                7
    HEAD       404               10
    HEAD       499               13
    HEAD       500                3
    HEAD       503               11
    PUT        200               10
    PUT        204                6
    PUT        400               11
    PUT        404               11
    PUT        499                8
    PUT        500               15
    PUT        503                6

(This is randomly-generated data, which is why you see some combinations that
probably don't make sense, like a 200 from a DELETE.)

You can specify multiple fields separated by commas, like above, or using "-b"
more than once.  This example does the same thing as the previous one:

    $ dn scan-file -b req.method -b res.statusCode \
        ./tests/data/2014/05-02/one.log 
    REQ.METHOD RES.STATUSCODE VALUE
    DELETE     200                6
    DELETE     204               12
    DELETE     400               12
    DELETE     404                8
    DELETE     499               12
    DELETE     500                8
    DELETE     503               13
    GET        200               12
    GET        204                7
    GET        400               11
    GET        404                6
    GET        499               10
    GET        500                7
    GET        503                5
    HEAD       200                7
    HEAD       204                5
    HEAD       400                7
    HEAD       404               10
    HEAD       499               13
    HEAD       500                3
    HEAD       503               11
    PUT        200               10
    PUT        204                6
    PUT        400               11
    PUT        404               11
    PUT        499                8
    PUT        500               15
    PUT        503                6

The order of breakdowns matters.  If we reverse them, we get different output:

    $ dn scan-file -b res.statusCode,req.method ./tests/data/2014/05-02/one.log
    RES.STATUSCODE REQ.METHOD VALUE
    200            DELETE         6
    200            GET           12
    200            HEAD           7
    200            PUT           10
    204            DELETE        12
    204            GET            7
    204            HEAD           5
    204            PUT            6
    400            DELETE        12
    400            GET           11
    400            HEAD           7
    400            PUT           11
    404            DELETE         8
    404            GET            6
    404            HEAD          10
    404            PUT           11
    499            DELETE        12
    499            GET           10
    499            HEAD          13
    499            PUT            8
    500            DELETE         8
    500            GET            7
    500            HEAD           3
    500            PUT           15
    503            DELETE        13
    503            GET            5
    503            HEAD          11
    503            PUT            6

### Filters

You can filter records using [node-krill](https://github.com/joyent/node-krill)
filter syntax:

    $ dn scan-file -f '{ "eq": [ "req.method", "GET" ] }' \
        ./tests/data/2014/05-02/one.log
    VALUE
       58

and you can combine this with breakdowns, of course:

    $ dn scan-file -f '{ "eq": [ "req.method", "GET" ] }' \
          -b operation ./tests/data/2014/05-02/one.log
    OPERATION        VALUE
    getjoberrors        17
    getpublicstorage    14
    getstorage          27


### Numeric breakdowns

To break down by numeric quantities, it's usually best to aggregate nearby
values into buckets.  Here's a histogram of the "latency" field from this log:

    $ dn scan-file -b latency[aggr=quantize] ./tests/data/2014/05-02/one.log

               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@                                       11
                   2 |@@@@@@@@@                                55
                   4 |@@@@@@@@                                 50
                   8 |                                         0
                  16 |@@@@@@@@@                                58
                  32 |                                         0
                  64 |@@                                       10
                 128 |@@@                                      18
                 256 |                                         0
                 512 |                                         0
                1024 |@@                                       14
                2048 |@@@@@@                                   36
                4096 |                                         0

"aggr=quantize" specifies a power-of-two bucketization.  You can also do a
linear quantization, say with steps of size 50 (notice the quotes):

    $ dn scan-file -b 'latency[aggr=lquantize;step=50]' \
        ./tests/data/2014/05-02/one.log

               value  ------------- Distribution ------------- count
                   0 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@             174
                  50 |                                         0
                 100 |@@                                       15
                 150 |@@                                       13
                 200 |                                         0
                 250 |                                         0
                 300 |                                         0
                 350 |                                         0
                 400 |                                         0
                 450 |                                         0
                 500 |                                         0
                 550 |                                         0
                 600 |                                         0
                 650 |                                         0
                 700 |                                         0
                 750 |                                         0
                 800 |                                         0
                 850 |                                         0
                 900 |                                         0
                 950 |                                         0
                1000 |                                         1
                1050 |                                         0
                1100 |                                         1
    ...

These are modeled after DTrace's aggregating actions.  You can combine these
with other breakdowns:

    $ dn scan-file -f '{ "eq": [ "req.method", "GET" ] }' \
          -b req.method,operation,latency[aggr=quantize] \
          ./tests/data/2014/05-02/one.log
    GET, getjoberrors
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |@@@@@@@                                  3
                   4 |@@@@@                                    2
                   8 |                                         0
                  16 |@@@@@@@@@@@@@@@@                         7
                  32 |                                         0
                  64 |@@@@@                                    2
                 128 |                                         0
                 256 |                                         0
                 512 |                                         0
                1024 |@@                                       1
                2048 |@@@@@                                    2
                4096 |                                         0

    GET, getpublicstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |@@@@@@                                   2
                   4 |@@@@@@@@@@@@@@@@@                        6
                   8 |                                         0
                  16 |@@@@@@@@@                                3
                  32 |                                         0
                  64 |                                         0
                 128 |@@@                                      1
                 256 |                                         0
                 512 |                                         0
                1024 |@@@                                      1
                2048 |@@@                                      1
                4096 |                                         0

    GET, getstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |                                         0
                   2 |@@@@@@@@@@                               7
                   4 |@@@@@@                                   4
                   8 |                                         0
                  16 |@@@@@@@@@@@@@                            9
                  32 |                                         0
                  64 |@                                        1
                 128 |@                                        1
                 256 |                                         0
                 512 |                                         0
                1024 |@@@                                      2
                2048 |@@@@                                     3
                4096 |                                         0

If the last field isn't an aggregation, "dn" won't print a histogram, but it
will still group nearby values.  For example, if we reverse the order of that
last example:

    $ dn scan-file -f '{ "eq": [ "req.method", "GET" ] }' \
          -b latency[aggr=quantize],req.method,operation \
          ./tests/data/2014/05-02/one.log
    LATENCY REQ.METHOD OPERATION        VALUE
          2 GET        getjoberrors         3
          2 GET        getpublicstorage     2
          2 GET        getstorage           7
          4 GET        getjoberrors         2
          4 GET        getpublicstorage     6
          4 GET        getstorage           4
         16 GET        getjoberrors         7
         16 GET        getpublicstorage     3
         16 GET        getstorage           9
         64 GET        getjoberrors         2
         64 GET        getstorage           1
        128 GET        getpublicstorage     1
        128 GET        getstorage           1
       1024 GET        getjoberrors         1
       1024 GET        getpublicstorage     1
       1024 GET        getstorage           2
       2048 GET        getjoberrors         2
       2048 GET        getpublicstorage     1
       2048 GET        getstorage           3

You can get per-hour results by specifying a "date" field and doing a linear
quantization with steps of size 3600 (for 3600 seconds per hour).  When using a
"date" field, you have to specify what underlying JSON field should be parsed
as a date:

    $ dn scan-file -b 'timestamp[date;field=time;aggr=lquantize;step=3600]' \
        -b req.method ./tests/data/2014/05-02/one.log
    TIMESTAMP                REQ.METHOD VALUE
    2014-05-02T00:00:00.000Z DELETE         2
    2014-05-02T00:00:00.000Z GET            4
    2014-05-02T00:00:00.000Z PUT            5
    2014-05-02T01:00:00.000Z DELETE         3
    2014-05-02T01:00:00.000Z GET            3
    2014-05-02T01:00:00.000Z HEAD           1
    2014-05-02T01:00:00.000Z PUT            3
    2014-05-02T02:00:00.000Z DELETE         5
    2014-05-02T02:00:00.000Z GET            1
    2014-05-02T02:00:00.000Z HEAD           2
    2014-05-02T02:00:00.000Z PUT            3
    2014-05-02T03:00:00.000Z DELETE         3
    2014-05-02T03:00:00.000Z GET            3
    2014-05-02T03:00:00.000Z HEAD           2
    2014-05-02T03:00:00.000Z PUT            2
    ...
    2014-05-02T22:00:00.000Z DELETE         3
    2014-05-02T22:00:00.000Z GET            1
    2014-05-02T22:00:00.000Z HEAD           4
    2014-05-02T22:00:00.000Z PUT            2
    2014-05-02T23:00:00.000Z DELETE         3
    2014-05-02T23:00:00.000Z GET            3
    2014-05-02T23:00:00.000Z HEAD           3
    2014-05-02T23:00:00.000Z PUT            1


### Indexes

All of the examples above used a full file scan just to demonstrate the data
model.  The point of Dragnet is to create indexes that can answer these same
queries much faster.  You can index a file much the way you write a query.
Here's an example that creates indexes on the request method, operation, and
latency:

    $ dn index-file -c req.method,operation,latency[aggr=quantize] \
          ./tests/data/2014/05-02/one.log myindex
    index "myindex" created

My sample data is fairly small, but the index is much smaller:

    $ ls -lh ./tests/data/2014/05-02/one.log 
    -rw-r--r--  1 dap  staff    55K Jul 29 15:21 ./tests/data/2014/05-02/one.log

    $ ls -lh myindex 
    -rw-r--r--  1 dap  staff   8.0K Jul 29 15:47 myindex

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
    PUT|putobject|2|7
    PUT|putobject|4|5
    PUT|putobject|16|4
    PUT|putobject|64|1
    PUT|putobject|128|4
    PUT|putobject|1024|1
    PUT|putobject|2048|1
    PUT|putpublicobject|1|5
    PUT|putpublicobject|2|4
    PUT|putpublicobject|4|8
    ...

You can query an index with the same syntax you'd use for scanning, but with the
"query-file" command:

    $ dn query-file -f '{ "eq": [ "req.method", "GET" ] }' \
          -b latency[aggr=quantize],req.method,operation myindex 
    LATENCY REQ.METHOD OPERATION        VALUE
          2 GET        getjoberrors         3
          2 GET        getpublicstorage     2
          2 GET        getstorage           7
          4 GET        getjoberrors         2
          4 GET        getpublicstorage     6
          4 GET        getstorage           4
         16 GET        getjoberrors         7
         16 GET        getpublicstorage     3
         16 GET        getstorage           9
         64 GET        getjoberrors         2
         64 GET        getstorage           1
        128 GET        getpublicstorage     1
        128 GET        getstorage           1
       1024 GET        getjoberrors         1
       1024 GET        getpublicstorage     1
       1024 GET        getstorage           2
       2048 GET        getjoberrors         2
       2048 GET        getpublicstorage     1
       2048 GET        getstorage           3

Query and scan should return the same results -- the point is that query should
be much faster.


### Beyond files

All of the examples used a single data file and a single index file to
demonstrate the main ideas, but Dragnet is designed for larger corpuses with
many files.  Your data set can have as many files as you want, and Dragnet
creates per-hour index files by default using the "time" field in each JSON
object.

Here's a directory with two files, each containing three hours' worth of random
data:

    $ find tests/data -type f
    tests/data/2014/05-01/one.log
    tests/data/2014/05-01/two.log
    tests/data/2014/05-02/one.log
    tests/data/2014/05-02/two.log
    tests/data/2014/05-03/one.log
    tests/data/2014/05-03/two.log
    tests/data/2014/05-04/one.log
    tests/data/2014/05-04/two.log
    tests/data/2014/05-05/more.log

You can scan the entire directory tree by using "scan-tree" instead of
"scan-file":

    $ dn scan-tree ./tests/data
    VALUE
     2252

    $ dn scan-tree -b req.method ./tests/data
    REQ.METHOD VALUE
    DELETE       582
    GET          556
    HEAD         551
    PUT          563

You can index it the same way:

    $ dn index-tree -c 'timestamp[date;field=time;aggr=lquantize;step=86400]' \
         -c req.method,res.statusCode,latency[aggr=quantize] \
         ./tests/data data_index
    indexes created

    $ find data_index -type f 
    data_index/by_hour/2014-05-01-00.sqlite
    data_index/by_hour/2014-05-01-01.sqlite
    data_index/by_hour/2014-05-01-02.sqlite
    data_index/by_hour/2014-05-01-03.sqlite
    ...
    data_index/by_hour/2014-05-05-21.sqlite
    data_index/by_hour/2014-05-05-22.sqlite
    data_index/by_hour/2014-05-05-23.sqlite

Notice there are many index files: one for each hour of data from the original
data set.  The number of indexes doesn't depend on the size or number of input
files.  You never need to worry about the number of index files, though.  "dn"
takes care of searching whichever set of them need to be searched.

You can query these indexes using "query-tree" and specifying the index
directory:

    $ dn query-tree -b req.method data_index
    REQ.METHOD VALUE
    DELETE       580
    GET          556
    HEAD         551
    PUT          563


### Dragnet on Manta

Dragnet supports operating directly on data stored in Joyent's [Manta Storage
Service](https://apidocs.joyent.com/manta/).  When working with Manta:

* Raw data is read from Manta objects rather than local files.
* Data operations (scanning, indexing, and querying) are executed in Manta
  compute jobs to avoid copying data out of the object store.  Only the final
  results of scan and query operations are downloaded so they can be printed by
  the "dn" command.
* You can still use the --time-format, --before, and --after options to prune
  directories to search when scanning, indexing, or querying.
* You're responsible for cost of storing data and running compute jobs on Manta.

To use Dragnet on Manta, first set up the Manta CLI tools using the [Manta
"Getting Started"
instructions](https://apidocs.joyent.com/manta/index.html#getting-started).  You
need to set MANTA\_URL, MANTA\_USER, and MANTA\_KEY\_ID as you would for the
rest of the Manta command-line tools.  If "mls" works, you're good to go.

For sample data, there's a Manta copy of the test data shipped with Dragnet in
/dap/public/dragnet/testdata.  You can scan it with "scan-manta", which works
just like "scan-tree" except that the argument is a path in Manta rather than a
path to a local directory:

    $ dn scan-manta /dap/public/dragnet/testdata
    using existing asset: "/dap/public/dragnet/assets/dragnet-0.0.0.tgz"
    submitted job 9ed83408-0a41-c6b7-ebde-8c2d3d8f1a3c
    submitted 9 inputs
    VALUE
     2252

Similarly, you can run "index-manta" to index data stored in Manta, and its
arguments are just like "index-tree", but the paths represent Manta paths rather
than local filesystem paths:

    $ dn index-manta -c 'timestamp[date;field=time;aggr=lquantize;step=86400]' \
        -c req.method,res.statusCode --interval=day \
	/dap/public/dragnet/testdata /dap/stor/dragnet_test_index
    using existing asset: "/dap/public/dragnet/assets/dragnet-0.0.0.tgz"
    submitted job 8cd54704-5501-cae6-9dbd-e9a84c0a9146
    submitted 9 inputs
    indexes created

    $ mfind -t o /dap/stor/dragnet_test_index
    /dap/stor/dragnet_test_index/by_day/2014-05-01.sqlite
    /dap/stor/dragnet_test_index/by_day/2014-05-02.sqlite
    /dap/stor/dragnet_test_index/by_day/2014-05-03.sqlite
    /dap/stor/dragnet_test_index/by_day/2014-05-04.sqlite
    /dap/stor/dragnet_test_index/by_day/2014-05-05.sqlite

And you can query it with "query-mjob":

    $ dn query-mjob /dap/stor/dragnet_test_index
    using existing asset: "/dap/public/dragnet/assets/dragnet-0.0.0.tgz"
    submitted job ddf8b4cc-f804-4899-e857-876a293f37b0
    submitted 5 inputs
    VALUE
     2250

    $ dn query-mjob -b req.method /dap/stor/dragnet_test_index
    using existing asset: "/dap/public/dragnet/assets/dragnet-0.0.0.tgz"
    submitted job 4483dbf4-341e-4984-bf10-a6bae004001d
    submitted 5 inputs
    REQ.METHOD VALUE
    DELETE       580
    GET          556
    HEAD         551
    PUT          563

(It's "query-mjob" rather than "query-manta" because there may be other
Manta-based query commands in the future.)


## Reference

If you don't already know what "dn" does, you're better off starting with the
"Getting Started" section above.

### Scanning raw data

General forms:

    dn scan-file  [-b|--breakdowns COLUMN[,COLUMN...]]
                  [-f|--filter FILTER]
                  [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                  [--time-field=FIELDNAME]
                  [--data-format json|json-skinner]
                  [--counters] [--points] [--warnings]
                  DATA_FILE

    dn scan-tree  [-b|--breakdowns COLUMN[,COLUMN...]]
                  [-f|--filter FILTER]
                  [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                  [--time-field=FIELDNAME]
                  [--data-format json|json-skinner]
                  [--time-format=TIME_FORMAT] 
                  [--counters] [--points] [--warnings]
                  DATA_DIRECTORY

    dn scan-manta [-b|--breakdowns COLUMN[,COLUMN...]]
                  [-f|--filter FILTER]
                  [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                  [--time-field=FIELDNAME]
                  [--data-format json|json-skinner]
                  [--time-format=TIME_FORMAT] 
                  [--counters] [--points] [--warnings]
                  DATA_DIRECTORY

Scan the records in a single newline-separated-JSON data file:

    dn scan-file SCAN_OPTIONS data_file

Scan the records in all files in "data\_directory":

    dn scan-tree SCAN_OPTIONS data_directory

Scan the records in all Manta objects under "/$MANTA\_USER/stor/my\_data":

    dn scan-manta SCAN_OPTIONS "/$MANTA_USER/stor/my_data"

Scan only data from the first few days of July, assuming data is laid out under
"data\_directory/YYYY/MM/DD":

    dn scan-tree SCAN_OPTIONS --time-format=%Y/%m/%d
        --after 2014-07-01 --before 2014-07-04
        data_directory

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
* `--warnings`: as data is scanned, show warnings about records that are
  dropped.  Common reasons include: filtered out by a --filter filter, filtered
  out by --before or --after, failed to evaluate the --filter (e.g., because a
  field specified in the filter isn't present), failed to parse a numeric field
  (e.g., a field with "aggr"), or failed to parse a timestamp field.  As with
  --counters, everything about this option's output is unstable and subject to
  change at any time.

### Indexing

General forms:

    dn index-file   [-c|--columns COLUMN[,COLUMN...]]
                    [-f|--filter FILTER]
                    [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                    [--data-format json|json-skinner]
                    [-i|--interval hour|day]
                    [--counters] [--warnings]
                    DATA_FILE INDEX_FILE

    dn index-tree   [-c|--columns COLUMN[,COLUMN...]]
                    [-f|--filter FILTER]
                    [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                    [--time-format=TIME_FORMAT]
                    [--data-format json|json-skinner]
                    [-i|--interval hour|day]
                    [--counters] [--warnings]
                    DATA_DIRECTORY INDEX_DIRECTORY

    dn rollup-tree  [-c|--columns COLUMN[,COLUMN...]]
                    [-f|--filter FILTER]
                    [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                    [-i|--interval hour|day]
                    [-s|--source hour]
                    [--counters] [--warnings]
                    INDEX_DIRECTORY

    dn index-manta  [-c|--columns COLUMN[,COLUMN...]]
                    [-f|--filter FILTER]
                    [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                    [--time-format=TIME_FORMAT]
                    [--data-format json|json-skinner]
                    [-i|--interval hour|day]
                    [--counters] [--warnings]
                    DATA_DIRECTORY INDEX_DIRECTORY

Generate a single index file from a single newline-separated-JSON data file:

    dn index-file INDEX_OPTIONS data_file index_file

Generate hourly index files into "index\_directory" from data stored in
"data\_directory":

    dn index-tree INDEX_OPTIONS data_directory index_directory

Generate daily index files instead:

    dn index-tree INDEX_OPTIONS --interval=day data_directory index_directory

Generate hourly indexes, but only for the first few days of July, assuming data
is laid out under "data\_directory/YYYY/MM/DD"

    dn index-tree INDEX_OPTIONS 
        --time-format=%Y/%m/%d --after 2014-07-01 --before 2014-07-04
        data_directory index_directory

Generate daily indexes from hourly indexes:

    dn rollup-tree INDEX_OPTIONS --source=hour index_directory

INDEX\_OPTIONS include:

* `-c | --columns COLUMN[,COLUMN]`: Same as columns for "dn scan --breakdowns".
* `-f | --filter FILTER`: Same as "dn scan --filter".
* `--after START_TIMESTAMP`: Same as "dn scan --after".
* `--before END_TIMESTAMP`: Same as "dn scan --before".
* `--time-format TIME_FORMAT`: Same as "dn scan --time-format".  This only
  applies to --index-tree.
* `--data-format json | json-skinner`: Same as "dn scan --data-format".  This
  only applies to --index-file and --index-tree.
* `-i | --interval INTERVAL`: Specifies that indexes should be chunked into
  files by INTERVAL, which is either "hour" or "day".  This is only supported
  for "index-tree" and "rollup-tree".  The default is "hour".
* `-s | --source hour`: Specifies that the underlying data for the index
  should come from hourly indexes instead of the raw data files, which is useful
  to build daily indexes more efficiently.  This only applies to rollup-tree.

To specify the time resolution of each index file, you specify your own
"timestamp" column.  For example, specifying column
`timestamp[date;field=time;aggr=lquantize;step=60]` adds a field called
"timestamp" to the index which is the result of parsing the "time" field in the
raw data as an ISO 8601 timestamp and converting that to a Unix timestamp
(seconds since the epoch).  The result is bucketed by minute (`step=60`).  If
you want the resolution to be 10 seconds instead, use `step=10`.

There are a few debugging options:

* `--counters`: See "dn scan --counters".
* `--warnings`: See "dn scan --warnings".

When using forms "dn index-tree", you must include at least one column that's a
"date" field.  That field will be used to figure out which hourly or daily index
file a given data point should wind up in.


### Querying

"dn query-file" and "dn query-tree" support arguments like "dn scan-file" and
"dn scan-tree":

    dn query-file  [-b|--breakdowns COLUMN[,COLUMN...]]
                   [-f|--filter FILTER]
                   [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                   [--time-field TIME_FIELD]
                   [--points] [--counters]
                   INDEX_FILE

    dn query-tree  [-b|--breakdowns COLUMN[,COLUMN...]]
                   [-f|--filter FILTER]
                   [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                   [--time-field TIME_FIELD]
                   [--points] [--counters]
                   INDEX_DIRECTORY

    dn query-manta [-b|--breakdowns COLUMN[,COLUMN...]]
                   [-f|--filter FILTER]
                   [--before END_TIMESTAMP] [--after START_TIMESTAMP]
                   [--time-field TIME_FIELD]
                   [--points] [--counters]
                   INDEX_DIRECTORY

All of these options work just as documented for "dn scan-file" and "dn
scan-tree".  "INDEX\_FILE" should be a single index file to be queried.
INDEX\_DIRECTORY refers to a directory of indexes created with "dn index-tree"
or "dn index-manta".  "dn" will automatically select the daily indexes if
available and fall back to hourly indexes if not.

Several scan-related arguments are not supported by when querying because they
don't apply:

* `--data-format` doesn't apply because the format of indexes is fixed.
* `--time-format` doesn't apply because the structure of the index directory
  tree is fixed.
* `--warnings` doesn't apply because any problems parsing indexes is
  considered a fatal error.

The fact that --time-field is ever necessary for queries is a bug.


## Memory usage

Dragnet is currently limited by the maximum size of the V8 heap, and the Manta
version uses a 32-bit binary.  The limit is not affected by the number of
*input* data points, but the number of unique tuples.  If you're just counting
records, you can process an arbitrary number of data points.  If you're indexing
10 fields, each of which can have 10 different values (all independently),
that's 10 billion output tuples, which is more than Dragnet can currently
handle.

There's no built-in limit on the number of unique tuples, or the number of
allowed values for each field, so it's easy to accidentally exceed this limit by
selecting a field that has a lot of different values.  When you exceed this
limit, the failure mode is not good.  The program will usually start running
extremely slowly for a while as V8 tries to collect lots of garbage, and
eventually the program will crash (hopefully dumping core) with a message about
a memory allocation failure.

To deal with this, you have to reduce the number of unique tuples that Dragnet
has to keep track of.  You can do this in a few ways:

* First, check that you didn't forget to aggregate some numeric fields.  If you
  try to index the timestamp without aggregating, you'll get per-second data,
  which is likely to produce way too many unique tuples.  Aggregate per-minute
  instead.
* Give up some resolution on numeric fields.  Instead of bucketing per 10
  seconds, bucket per-minute.  Instead of latency in groups of 10 milliseconds,
  use power-of-two buckets.
* Select fewer columns.  In the above example with 10 columns, skipping one
  column reduces the number of unique data points by a factor of 10.
* Relatedly: instead of using one index with 10 columns, use several indexes
  with only a couple of columns each.  You rarely need to filter and break down
  using all possible fields, so create specific indexes for the reports you
  want.
* If you only run into this problem while indexing, try indexing less data at
  once.  If you're generating daily indexes, restrict each "index" operation to
  a day's worth of input data, and run separate operations for each day.


## Common issues

#### "dn" dumps core with a message about memory allocation failed

See "Memory usage" above.


#### Some data is missing

* If you're using a filter, check that you didn't accidentally filter out the
  records.
* If you specified a "date" field for a scan or index operation, check that the
  field is present and parseable as a date.
* If you're aggregating on a numeric field (e.g., you used "aggr=quantize" or
  "aggr=lquantize"), check that the field is present and actually numeric.
  Strings representing numbers (like "123") don't count.  Records are dropped
  where Dragnet finds anything but a number where a numeric field is required,
  so if you configure it wrong, you'll be left with no records.

It may help to check all of these by running with `--counters` or `--warnings`.
Counters will show how many records make it through each stage of the data
processing pipeline, and warnings should print out a warning when records are
dropped.


#### "dn" exits 0 without producing any output

See "Some data is missing".  In many of those cases, the problem ends up
applying to all records and all of them get dropped.


#### "dn" reports a premature exit

This is always a bug.  It means Node exited before "dn" expected it to, which
usually means a missed callback.


## A note on performance

While the architecture is designed for scalability, no serious performance work
has been done on the implementation.  There's currently a lot of startup cost.
