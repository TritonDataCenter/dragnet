# Dragnet

"dn" is a tool for analyzing event stream data stored in files.  There are three
commands:

* "scan": scan over raw data to execute a query
* "index": scan over raw data to produce an index
* "query": search indexes to execute a query

The point is to "index" data and then "query" it.  "scan" is available to check
results and to run ad-hoc queries that it may not make sense to index.

## Synopsis

dragnet only supports newline-separated JSON.  Try it on muskie log data.  In
the simplest mode, dragnet operates on raw files.  With no arguments, "scan"
just counts records:

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

Although it's a little janky, you can get per-interval results by aggregating on
the built-in "\_\_dn\_ts" field.  This is a synthetic field added by "dn" based
on parsing the "time" field of each JSON object.  It's a Unix timestamp.  **This
is an implementation detail, so don't hardcode it into anything**.  It will be
replaced with a better interface.  But for an example, here's how you can get
per-hour data from a 3-hour log file:

    $ dn scan -b '__dn_ts[aggr=lquantize;step=3600],req.method' mydata.log
       __DN_TS REQ.METHOD VALUE
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
always creates per-hour index files using the "time" field in each JSON object.

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

- Add first-class support for pruning based on timestamps for "scan", "index",
  and "query".  This requires specifying how files are named, but would allow
  you to scan or index only a subset of data.  Once that's built into queries,
  we can use that to query only the indexes we need.
- Add support for rolled-up indexes: at least daily, weekly, and monthly.  This
  is important, since it reduces by an order of magnitude (or more) the number
  of indexes needed to query for execute queries over even modest intervals
  (e.g., a day).
- Add support for scanning, indexing, and querying data stored in Manta.
- Verify that performance for indexed scans is good.

In the long run, we'll want to add support for other data formats as well.


# Background

**This prototype (along with the rest of this doc) is still in early research
and prototype stages!**

### The goal

Muskie audit log entries contain several fields, including:

* timestamp (string representing a number with a large set of values)
* hostname servicing the request (string)
* request method (string)
* response code (number with a small set of values)
* latency to first byte (number with a large set of values)
* total latency (number with a large set of values)

There are a couple of reports we'd like to be able to generate quickly, for an
arbitrary time period (covering several seconds to several months)

| **Graph**                           | **Kind**         | **Notes** |
|:----------------------------------- |:---------------- |:- |
| total requests                      | line graph       | |
| total requests by status code       | multi line graph | |
| error rate                          | line graph       | need to divide request counts |
| error rate by instance              | multi line graph | need to divide request counts and break down by another field |
| 99th percentile of request latency  | line graph       | estimate based on quantized latency data |
| requests by latency                 | heat map         | |
| requests by latency and status code | multi heat map   | |
| requests by latency and hostname    | multi heat map   | |

### Indexing

To accomplish this, we'll build a single index that basically looks like a
traditional RDBMS table with columns "timestamp" (truncated according to a
prespecified resolution), "hostname", "request method", "request URL", "response
code" (all simple strings), "latency to first byte" (as a quantized bucket), and
"total latency" (as a quantized bucket).  In addition, the index implicitly
stores "count" -- the count of records matching each unique combination of the
other columns.

So this request from the raw data (with unrelated parts elided):

```json
{
  "time": "2014-04-12T02:00:01.397Z",
  "hostname": "c84b3cab-1c20-4566-a880-0e202b6b63dd",
  "req": { "method": "GET" },
  "res": { "statusCode": 200 },
  "latency": 10,
  "latencyToFirstByte": 9
}
```

might be represented in this row:

```json
{
    "timestamp": "2014-04-12T02:00:00.000Z" /* 10-second window */
    "hostname": "c84b3cab-1c20-4566-a880-0e202b6b63dd",
    "req.method": "GET",
    "res.statusCode" 200,
    "latency": 3,               /* third power-of-two latency bucket */
    "latencyToFirstByte": 3,    /* ditto */
    "count": 57
}
```

along with 56 other "GET" requests serviced by the same hostname with a "200"
response in roughly the same latency during the same 10-second window.  (The
index isn't actually stored in JSON, but it's shown that way above for
clarity.)

### Building the index

You build the index using this configuration:

```json
{
    "name": "muskie.requests",
    "mantaroot": "/poseidon/stor/logs/muskie",
    "format": "json",
    "filter": { "eq": [ "audit", true ] },
    "primaryKey": "time",
    "columns" [
        "hostname",
        "req.method",
        "req.statusCode",
        {
            "field": "latency",
            "aggr": "quantize"
        },
        {
            "field": "latencyToFirstByte",
            "aggr": "quantize"
        }
    ]
}
```

### Querying the index

To count total requests in May, you'd submit a query that looks like this:

```json
{
    "index": "muskie.requests",
    "timeStart": "2014-05-01",
    "timeEnd": "2014-06-01"
}
```

To break out the results by status code (which would allow you to compute the
error rate):

```json
{
    "index": "muskie.requests",
    "timeStart": "2014-05-01",
    "timeEnd": "2014-06-01",
    "breakdowns": [ "req.statusCode" ]
}
```

and to break *those* down by instance, too:

```json
{
    "index": "muskie.requests",
    "timeStart": "2014-05-01",
    "timeEnd": "2014-06-01",
    "breakdowns": [ "hostname", "req.statusCode" ]
}
```

To count total requests for a specific hour in May, broken out into per-minute
data points, you'd submit:

```json
{
    "index": "muskie.requests",
    "timeStart": "2014-05-07T03:00:00Z",
    "timeEnd": "2014-05-07T04:00:00Z",
    "timeResolution": 60,
}
```

To count errors during that same interval:

```json
{
    "index": "muskie.request",
    "timeStart": "2014-05-07T03:00:00Z",
    "timeEnd": "2014-05-07T04:00:00Z",
    "timeResolution": 60,
    "filter": { "ge": [ "res.statusCode", 500 ] },
}
```

You can get the latency results (which would be used to produce heat maps) by
adding that to "breakdowns".


## Index definitions

To create an index on data already stored in Manta, you need to specify:

### name (string)
e.g., `"muskie.requests"`

Unique identifier for this index.  This cannot be changed later.


### mantaroot (string)
e.g., `"/poseidon/stor/logs/muskie"`

Path in Manta where the data is stored

### directoryStructure (string, optional)
e.g., `"$year/$month/$day/$hour/$instance.log"`

Specifies how the Manta objects are organized so that you can specify which
files to index.  You're expected to use some combination of variables $year,
$month, $day, $hour, and $instance.  The string will be pattern-matched
against directories and objects that are found under "mantaroot".  This is
only used to let you specify which data to index.  For example, if you
specify $year, $month, and $day in this string, then you'll be able to index
only a given date range's worth of data (without having to scan everything).
If you specify nothing here, then you can only build an index on all data
under "mantaroot".

### format (string)
e.g., `"json"`

Specifies the format of data stored under "mantaroot".  The first supported
format is "json".  The format defines how the data is divided into records
as well as how records are modeled for use in the "filter" and "columns"
fields below.  For example, the "json" format assumes records are separated
by newlines and that all records are non-null objects (i.e., not primitive
types).  Properties in the JSON object are available as fields to "filter"
and "columns".

### filter (object)
e.g., `{ "eq": [ "audit", "true" ] }`

Specifies which records to include in this index.  All other records will be
dropped.  The format is a [node-krill](https://github.com/joyent/node-krill)
object.

### primaryKey (string)
e.g., `"time"`

Defines the name and field denoting the primary key that organizes the data.
This is almost always a time-based field, and for bunyan-style records it's
just called "time".

### primaryKeyType (string, optional)
e.g, `"timestamp"`

Defines the format of the primary key, which is usually a timestamp.
"timestamp" refers to an ISO 8601-format timestamp.  This is used to
bucketize data appropriately.

### columns (array)
e.g.: `[ "hostname", "req.method", "res.statusCode" ]`

Specifies the columns in the index, which defines the kinds of queries
serviced by the index.  Once the index is built, you'll be able to quickly
run SQL-like queries based on these columns.

Each entry in the array defines the name of the column and how to get its
value from the raw data.  The simplest case is a plain string like
"hostname", which means that each record provides a "hostname" field which
should be taken as the value for this column.  This is equivalent to this
object:

```json
{
    "name": "hostname",
    "field": "hostname"
}
```

Basic columns have a "name" and either a "field" property, in which case the
value is obtained directly from the raw data point.  Indexes store a row for
every unique combination of these basic fields, which becomes very
impractical for fields which can have very large numbers of values (e.g., a
URL in a large namespace or a numeric quantity like latency in
microseconds).  For URLs, the index should likely store only the top values,
in which case you can define this by specifying:

```json
{
    "name": "req.url",
    "field": "req.url",
    "top": 100
}
```

For numeric quantities, you can instead specify that the numbers should be
bucketized, as with a power-of-two distribution:

```json
{
    "name": "latency",
    "aggr": "quantize",
    "field": "latency"
}
```

or a linear distribution:

```json
{
    "name": "latency",
    "aggr": "lquantize",
    "field": "latency",
    "min": 0,
    "max": 1000,
    "bucketsize": 10
}
```

The index implicitly also stores the primary key, which is currently assumed
to be a timestamp, in a reasonable resolution.  These semantics are
currently hardcoded but should not be depended upon: hourly indexes will
store per-second data; daily indexes will store per-10-minute data; and
weekly indexes will store per-hour data.

The index also implicitly stores the count of records for each unique
combination of the other fields.

## Queries

Queries can specify:

### index (string)
e.g., `"muskie.requests"`

Specifies which index to query.

### timeStart (ISO date string, possibly partial)
e.g., `"2014-05-01"`

Describes the start range of data to query (inclusive)

### timeEnd (ISO date string, possibly partial)
e.g., `"2014-06-01"`

Describes the end range of data to query (exclusive)

### timeResolution (number \[of seconds\], optional)
e.g., `300`

Describes how to group results.  `300` would denote 5-minute intervals.  If
unspecified, the results are grouped into one bucket.

### filter (object, optional)
e.g., `{ "ge": [ "req.statusCode", "500" ] }`

Additional filter to apply over rows in the index, in the same format as the
index filter itself.

### breakdowns (array)
e.g., `[ "hostname", "req.statusCode" ]`

Specifies how to break out the results, in addition to by time (see
timeResolution).
