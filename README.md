<style>
body {
	font-family: Palatino, Times, serif;
	font-size: large;
	margin-top: 40px;
	margin-left: 15%;
	margin-right: 15%;
	line-height: 1.5em;
}
h1,h2 {
	padding-top: 40px;
}
</style>
## Synopsis

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

### primaryKey (object)
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
