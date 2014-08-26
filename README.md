# Dragnet

Dragnet is a tool for analyzing event stream data stored in files.  There are
three main commands:

* scan: scan over *raw data* to execute a query
* build: scan over raw data to produce an index for quickly answering predefined
  queries
* query: search *indexes* to execute a query

The prototypical use case is analyzing request logs from a production service.
The workflow for Dragnet looks like this:

* Predefine a bunch of metrics you care about (like total request count,
  request count by server instance, request type, and so on).
* When you accumulate new logs (e.g., hourly or daily), you *build* the index.
* Whenever you want the values of those metrics, you *query* the index.  This
  might be part of a constantly-updating dashboard, a daily report, or a
  threshold-based alarm.
* If you want to gather new metrics, you can define them and rebuild.
* If you want to run a complex query just once, you can *scan* the raw data
  rather than adding the query as a metric.

**This project is still a prototype.**  The commands and library interfaces may
change incompatibly at any time!


## Getting started

dragnet only supports newline-separated JSON.  Try it on the sample data in
./tests/data.  Start by defining a new **datasource**:

    $ dn datasource-list -v
    DATASOURCE           LOCATION                                                   
    my_logs              file://home/dap/dragnet/dragnet/tests/data                 
        dataFormat: "json"

Now you can scan the data to count the total number of requests:

    $ dn scan my_logs
    VALUE
     2252

You can also break out counts, e.g., by request method:

    $ dn scan -b req.method my_logs
    REQ.METHOD VALUE
    DELETE       582
    GET          556
    HEAD         551
    PUT          563

You can break out results by more than one field:

    $ dn scan -b req.method,res.statusCode my_logs
    REQ.METHOD RES.STATUSCODE VALUE
    DELETE     200               75
    DELETE     204               87
    DELETE     400               94
    DELETE     404               85
    DELETE     499               83
    DELETE     500               79
    DELETE     503               79
    GET        200               77
    GET        204               83
    GET        400               84
    GET        404               74
    GET        499               79
    GET        500               73
    GET        503               86
    HEAD       200               71
    HEAD       204               85
    HEAD       400               66
    HEAD       404               77
    HEAD       499               88
    HEAD       500               88
    HEAD       503               76
    PUT        200               80
    PUT        204               79
    PUT        400               83
    PUT        404               88
    PUT        499               68
    PUT        500               83
    PUT        503               82

(This is randomly-generated data, which is why you see some combinations that
probably don't make sense, like a 200 from a DELETE.)

You can specify multiple fields separated by commas, like above, or using "-b"
more than once.  This example does the same thing as the previous one:

    $ dn scan -b req.method -b res.statusCode my_logs
    REQ.METHOD RES.STATUSCODE VALUE
    DELETE     200               75
    DELETE     204               87
    DELETE     400               94
    DELETE     404               85
    DELETE     499               83
    DELETE     500               79
    DELETE     503               79
    GET        200               77
    GET        204               83
    GET        400               84
    GET        404               74
    GET        499               79
    GET        500               73
    GET        503               86
    HEAD       200               71
    HEAD       204               85
    HEAD       400               66
    HEAD       404               77
    HEAD       499               88
    HEAD       500               88
    HEAD       503               76
    PUT        200               80
    PUT        204               79
    PUT        400               83
    PUT        404               88
    PUT        499               68
    PUT        500               83
    PUT        503               82

The order of breakdowns matters.  If we reverse them, we get different output:

    $ dn scan -b res.statusCode,req.method my_logs
    RES.STATUSCODE REQ.METHOD VALUE
    200            DELETE        75
    200            GET           77
    200            HEAD          71
    200            PUT           80
    204            DELETE        87
    204            GET           83
    204            HEAD          85
    204            PUT           79
    400            DELETE        94
    400            GET           84
    400            HEAD          66
    400            PUT           83
    404            DELETE        85
    404            GET           74
    404            HEAD          77
    404            PUT           88
    499            DELETE        83
    499            GET           79
    499            HEAD          88
    499            PUT           68
    500            DELETE        79
    500            GET           73
    500            HEAD          88
    500            PUT           83
    503            DELETE        79
    503            GET           86
    503            HEAD          76
    503            PUT           82


### Filters

You can filter records using [node-krill](https://github.com/joyent/node-krill)
filter syntax:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' my_logs
    VALUE
      556

and you can combine this with breakdowns, of course:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' -b operation my_logs
    OPERATION        VALUE
    getjoberrors       181
    getpublicstorage   176
    getstorage         199


### Numeric breakdowns

To break down by numeric quantities, it's usually best to aggregate nearby
values into buckets.  Here's a histogram of the "latency" field from this log:

    $ dn scan -b latency[aggr=quantize] my_logs

               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@                                       113
                   2 |@@@@@@@@                                 449
                   4 |@@@@@@                                   348
                   8 |                                         0
                  16 |@@@@@@@@@@@@                             682
                  32 |                                         0
                  64 |@                                        57
                 128 |@@@                                      165
                 256 |                                         0
                 512 |                                         0
                1024 |@@                                       136
                2048 |@@@@@                                    302
                4096 |                                         0

"aggr=quantize" specifies a power-of-two bucketization.  You can also do a
linear quantization, say with steps of size 200:

    $ dn scan -b latency[aggr=lquantize,step=200] my_logs

               value  ------------- Distribution ------------- count
                   0 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@         1814
                 200 |                                         0
                 400 |                                         0
                 600 |                                         0
                 800 |                                         0
                1000 |                                         23
                1200 |@                                        31
                1400 |@                                        35
                1600 |                                         18
                1800 |                                         24
                2000 |@                                        34
                2200 |@                                        35
                2400 |                                         28
                2600 |@                                        33
                2800 |                                         18
                3000 |@                                        34
                3200 |                                         27
                3400 |@                                        34
                3600 |                                         26
                3800 |                                         25
                4000 |                                         13
                4200 |                                         0

These are modeled after DTrace's aggregating actions.  You can combine these
with filters and other breakdowns:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' \
        -b req.method,operation,latency[aggr=quantize] my_logs
    GET, getjoberrors
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@                                       9
                   2 |@@@@@@@                                  32
                   4 |@@@@@                                    24
                   8 |                                         0
                  16 |@@@@@@@@@@@@@@                           63
                  32 |                                         0
                  64 |@                                        5
                 128 |@@@                                      13
                 256 |                                         0
                 512 |                                         0
                1024 |@@@                                      13
                2048 |@@@@@                                    22
                4096 |                                         0

    GET, getpublicstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@@                                      12
                   2 |@@@@@@@@                                 37
                   4 |@@@@@@                                   28
                   8 |                                         0
                  16 |@@@@@@@@@@@@                             51
                  32 |                                         0
                  64 |                                         1
                 128 |@@@@                                     17
                 256 |                                         0
                 512 |                                         0
                1024 |@@                                       9
                2048 |@@@@@                                    21
                4096 |                                         0

    GET, getstorage
               value  ------------- Distribution ------------- count
                   0 |                                         0
                   1 |@@                                       12
                   2 |@@@@@@@                                  37
                   4 |@@@@@@                                   29
                   8 |                                         0
                  16 |@@@@@@@@@@@@@                            67
                  32 |                                         0
                  64 |@@                                       9
                 128 |@@                                       8
                 256 |                                         0
                 512 |                                         0
                1024 |@@                                       11
                2048 |@@@@@                                    26
                4096 |                                         0

If the last field isn't an aggregation, "dn" won't print a histogram, but it
will still group nearby values.  For example, if we reverse the order of that
last example:

    $ dn scan -f '{ "eq": [ "req.method", "GET" ] }' \
        -b latency[aggr=quantize],req.method,operation my_logs
    LATENCY REQ.METHOD OPERATION        VALUE
          1 GET        getjoberrors         9
          1 GET        getpublicstorage    12
          1 GET        getstorage          12
          2 GET        getjoberrors        32
          2 GET        getpublicstorage    37
          2 GET        getstorage          37
          4 GET        getjoberrors        24
          4 GET        getpublicstorage    28
          4 GET        getstorage          29
         16 GET        getjoberrors        63
         16 GET        getpublicstorage    51
         16 GET        getstorage          67
         64 GET        getjoberrors         5
         64 GET        getpublicstorage     1
         64 GET        getstorage           9
        128 GET        getjoberrors        13
        128 GET        getpublicstorage    17
        128 GET        getstorage           8
       1024 GET        getjoberrors        13
       1024 GET        getpublicstorage     9
       1024 GET        getstorage          11
       2048 GET        getjoberrors        22
       2048 GET        getpublicstorage    21
       2048 GET        getstorage          26

You can get per-day results by specifying a "date" field and doing a linear
quantization with steps of size 86400 (for 86400 seconds per day).  When using
a "date" field, you have to specify what underlying JSON field should be parsed
as a date:

    $ dn scan -b timestamp[date,field=time,aggr=lquantize,step=86400] \
        -b req.method my_logs
    TIMESTAMP                REQ.METHOD VALUE
    2014-05-01T00:00:00.000Z DELETE       142
    2014-05-01T00:00:00.000Z GET          113
    2014-05-01T00:00:00.000Z HEAD         125
    2014-05-01T00:00:00.000Z PUT          120
    2014-05-02T00:00:00.000Z DELETE       133
    2014-05-02T00:00:00.000Z GET          120
    2014-05-02T00:00:00.000Z HEAD         125
    2014-05-02T00:00:00.000Z PUT          122
    2014-05-03T00:00:00.000Z DELETE       122
    2014-05-03T00:00:00.000Z GET          124
    2014-05-03T00:00:00.000Z HEAD         123
    2014-05-03T00:00:00.000Z PUT          131
    2014-05-04T00:00:00.000Z DELETE       128
    2014-05-04T00:00:00.000Z GET          120
    2014-05-04T00:00:00.000Z HEAD         127
    2014-05-04T00:00:00.000Z PUT          125
    2014-05-05T00:00:00.000Z DELETE        55
    2014-05-05T00:00:00.000Z GET           79
    2014-05-05T00:00:00.000Z HEAD          51
    2014-05-05T00:00:00.000Z PUT           65


### Indexes

All of the examples above used a full file scan just to demonstrate the data
model.  The point of Dragnet is to create indexes that can answer these same
queries much faster.

To build indexes, the datasource must have an index-path property, which tells
Dragnet where the index should be stored.  So let's remove the datasource we
added previously and re-add it with an index path.  We'll also specify a time
field, which Dragnet will use to build per-day indexes by default:

    $ dn datasource-remove my_logs
    $ dn datasource-add my_logs --path=$PWD/tests/data/ \
        --index-path=$PWD/my_index --time-field=time
    $ dn datasource-list -v
    DATASOURCE           LOCATION                                                   
    my_logs              file://home/dap/dragnet/dragnet/tests/data/                
        dataFormat: "json"
        indexPath:  "/home/dap/dragnet/dragnet/my_index"
        timeField:  "time"

Now we can start adding metrics for the datasource.  Let's say we want to be
able to quickly get the count of requests per minute, possibly broken out by
status code.  Let's add this metric:

    $ dn metric-add --datasource=my_logs \
        -b timestamp[field=time,date,aggr=lquantize,step=60] \
        -b res.statusCode requests_bystatus

Now build the index:

    $ dn build my_logs
    indexes for "my_logs" built

By default, "dn build" builds daily indexes.  You can see the individual files:

    $ find my_index -type f
    my_index/by_day/2014-05-01.sqlite
    my_index/by_day/2014-05-03.sqlite
    my_index/by_day/2014-05-05.sqlite
    my_index/by_day/2014-05-04.sqlite
    my_index/by_day/2014-05-02.sqlite

The indexes are much smaller than the original data, since they contain only
enough information to answer the queries.

You can query an index the same way you would scan the original data.
Generally, the query will be much faster, since it's not scanning the raw data.
Here's a count of all requests:

    $ dn query my_logs
    VALUE
     2250

Or just the server-side failures (status code at least 500):

    $ dn query --filter='{ "ge": [ "res.statusCode", 500 ] }' my_logs
    VALUE
      646

Or the failures by day:

    $ dn query --filter='{ "ge": [ "res.statusCode", 500 ] }' \
        -b timestamp[date,aggr=lquantize,step=86400] my_logs

                         value  ------------- Distribution ------------- count
      2014-05-01T00:00:00.000Z |@@@@@@@@@                                142
      2014-05-02T00:00:00.000Z |@@@@@@@@                                 132
      2014-05-03T00:00:00.000Z |@@@@@@@@@                                144
      2014-05-04T00:00:00.000Z |@@@@@@@@@@                               154
      2014-05-05T00:00:00.000Z |@@@@@                                    74
      2014-05-06T00:00:00.000Z |                                         0

Notice that you define metrics to build the index, but you don't need to query a
specific metric.  You can query anything that can be fetched from the data that
was gathered *for* those metrics.  If you ask for something that's not there,
you'll get an error:

    $ dn query -b req.method my_logs
    dn: index "/home/dap/dragnet/dragnet/my_index/by_day/2014-05-01.sqlite" query:
    no metrics available to serve query

But it will work if you add the metric and rebuild the index:

    $ dn metric-add --datasource=my_logs -b req.method my_logs

    $ dn build my_logs
    indexes for "my_logs" built

    $ dn query -b req.method my_logs
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
* As with files, you can use --time-format and --time-field when creating the
  data source, and then use --before and --after options to prune directories to
  search when scanning, indexing, or querying.  For large datasets, this saves
  an enormous amount of time just enumerating inputs.
* You're responsible for cost of storing data and running compute jobs on Manta.

To use Dragnet on Manta, first set up the Manta CLI tools using the [Manta
"Getting Started"
instructions](https://apidocs.joyent.com/manta/index.html#getting-started).  You
need to set MANTA\_URL, MANTA\_USER, and MANTA\_KEY\_ID as you would for the
rest of the Manta command-line tools.  If "mls" works, you're good to go.

For sample data, there's a Manta copy of the test data shipped with Dragnet in
/dap/public/dragnet/testdata.  You can scan add it like this:

    $ dn datasource-add dragnet_test_manta --backend=manta \
        --path=/dap/public/dragnet/testdata --time-field=time \

Then you can scan it just as with local data.  There's a little more debug
output in case you need to dig into the job:

    $ dn scan dragnet_test_manta
    using existing asset: "/manta/public/dragnet/assets/dragnet-0.0.2.tgz"
    submitted job 4a74af91-4b3d-c69b-e607-efe0c2911826
    submitted 9 inputs
    VALUE
     2252

Similarly, you can define metrics, build an index, and query it.  To do that, we
have to specify an index path, which must be somewhere in Manta you have access
to write:

    $ dn datasource-remove dragnet_test_manta

    $ dn datasource-add dragnet_test_manta --backend=manta --time-field=time \
        --path=/dap/public/dragnet/testdata \
        --index-path=/$MANTA_USER/stor/myindex

    $ dn metric-add --datasource=dragnet_test_manta \
        -b timestamp[date,field=time,aggr=lquantize,step=86400],req.method \
        by_method

Now we can build the index:

    $ dn build dragnet_test_manta
    using existing asset: "/manta/public/dragnet/assets/dragnet-0.0.2.tgz"
    submitted job 507242e7-7e76-6ae4-8ef4-cec1f9593909
    submitted 9 inputs
    indexes for "dragnet_test_manta" built

and query it:

    $ dn query dragnet_test_manta
    using existing asset: "/manta/public/dragnet/assets/dragnet-0.0.2.tgz"
    submitted job 66f20f4f-9d5d-68ae-a860-b4f1fedc9f53
    submitted 5 inputs
    VALUE
     2250

    $ dn query -b req.method dragnet_test_manta
    using existing asset: "/manta/public/dragnet/assets/dragnet-0.0.2.tgz"
    submitted job 39b4e5d1-2449-4529-fcbf-916cb885d979
    submitted 5 inputs
    REQ.METHOD VALUE
    DELETE       580
    GET          556
    HEAD         551
    PUT          563


## Reference

If you don't already know what "dn" does, you're better off starting with the
"Getting Started" section above.

### Scanning raw data

    dn scan [--before=START_TIME] [--after=END_TIME] [--filter=FILTER]
            [--breakdowns=BREAKDOWN[,...]]
            [--raw] [--points] [--counters] [--warnings] [--dry-run]
            [--assetroot=ASSET_ROOT] DATASOURCE

Scans all records in a datasource and aggregate the results.

The datasource specifies a backend (local files or Manta), a path to the files,
the file format, and a few option options describing how data is organized.  By
default, records must be newline-separated JSON.

The basic operation is counting records.  The assumption is that records
represent some useful metric (e.g., HTTP requests).  You can use --filter to
skip records.  You can use --breakdowns to break out the results by some field
(e.g., HTTP requests by request method).

Options include:

* `-b | --breakdowns COLUMN[,COLUMN...]`: A list of column definitions by which
  to break out the results.  With no breakdowns specified, the result of a scan
  is a count of all records scanned (excluding those dropped by the filter).
  With a breakdown on a column like "req.method" (request method, which is
  usually a string like "GET" or "PUT"), the result is a count for each value of
  "req.method" that was found.  With a breakdown on two columns, the result is a
  count for each unique combination of values for those columns (e.g., 15
  records with "req.method" equal to "GET" and "res.statusCode" equal to "200").
  To avoid exploding the number of results, you can group nearby values of
  numeric quantities using an aggregation.  See the tutorial above for details.
* `-f | --filter FILTER`: A node-krill (JSON format) predicate to evaluate on
  each record.  Records not matching the filter, as well as records missing
  fields that are used by the filter, are dropped.
* `--before END_TIMESTAMP`: Only scan data files containing data before
  END\_TIMESTAMP, and filter out data points after END\_TIMESTAMP (exclusive).
  Requires the datasource to have `--time-format` so that it can prune input
  files and `--time-field` so that it can filter records within each bucket.
* `--after START_TIMESTAMP`: Only scan data files containing data after
  START\_TIMESTAMP, and filter out data points before START\_TIMESTAMP
  (inclusive).  Requires the datasource to have `--time-format` so that it can
  prune input files and `--time-field` so that it can filter records within each
  bucket.

There are some options you specify when creating the datasource:

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

    dn build [--before=START_TIME] [--after=END_TIME]
             [--interval=hour|day|all] [--index-config=CONFIG_FILE]
             [--dry-run] [--assetroot=ASSET_ROOT]
             DATASOURCE

Generate a single index file from a single newline-separated-JSON data file:

    dn build --interval=all my_datasource

This generates an index capable of answering all of the metrics you've defined
on this datasource.

Generate daily index files (the default):

    dn build --interval=day my_datasource

Generate hourly indexes, but only for the first few days of July, assuming data
is laid out under "data\_directory/YYYY/MM/DD"

    dn build --interval=hour --after=2014-07-01 --before=2014-07-04

Options include:

* `--after START_TIMESTAMP`: Same as "dn scan --after".
* `--before END_TIMESTAMP`: Same as "dn scan --before".
* `-i | --interval INTERVAL`: Specifies that indexes should be chunked into
  files by INTERVAL, which is either "all", "hour" or "day".  The default is
  "day".

Like "scan", this uses several options on the datasource:

* `--time-format TIME_FORMAT`: See "dn scan".
* `--time-field TIME_FIELD`: See "dn scan".
* `--data-format json | json-skinner`: See "dn scan".

To specify the time resolution of a metric, specify your own "timestamp" column
with each metric.  For example, specifying column
`timestamp[date,field=time,aggr=lquantize,step=60]` adds a field called
"timestamp" to the index which is the result of parsing the "time" field in the
raw data as an ISO 8601 timestamp and converting that to a Unix timestamp
(seconds since the epoch).  The result is bucketed by minute (`step=60`).  If
you want the resolution to be 10 seconds instead, use `step=10`.


### Querying

    dn query [--before=START_TIME] [--after=END_TIME] [--filter=FILTER]
             [--breakdowns=BREAKDOWN[,...]] [--interval=hour|day|all]
             [--raw] [--points] [--counters]
             [--dry-run] [--assetroot=ASSET_ROOT]
             DATASOURCE

"dn query" is used just like "dn scan", but fetches data from the indexes built
by "dn build" rather than scanning the raw data every time.  The options are the
same as for "dn scan", with the addition of:

* `--interval all|hour|day`: scan the all-time, hourly, or daily indexes.  By
  default, scans daily indexes.

The `--data-format`, `--time-format`, and `--time-field` properties of the
datasource are not used when querying.


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
  column reduces the number of unique data points by a factor of 10.  You can do
  this by configuring more metrics with different fields (e.g., replace a single
  metric that includes timestamp, request method, and user agent with two that
  include timestamp and request method and (separately) timestamp and user
  agent.  These aren't exactly equivalent, but it's often sufficient.
* If you only run into this problem while indexing, try indexing less data at
  once.  If you're generating daily indexes, restrict each "build" operation to
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
