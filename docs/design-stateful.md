# Dragnet next steps

Dragnet is a good first cut at scanning data, indexing data, and querying the
indexes, but it's a little awkward to use:

* The obvious way to create an index to serve a bunch of queries doesn't scale
  very well.  You really need to create a bunch of indexes by hand in order to
  serve a bunch of different queries.  See
  [dragnet#22](https://github.com/joyent/dragnet/issues/22).  But this gets
  pretty unwieldy from the command-line.
* It would be nice to be able to "join" data sets as a post-processing operation
  (e.g., user uuids to user logins)

Instead of having stateless command-line operations that scan, query, or
index, let's define these statefully.

First, define some **data sources**, which include:

* a *backend* (e.g., filesystem or Manta)
* backend-specific parameters that include the *path* to the data and a
  strftime-like string describing how it's laid out (e.g., "%Y/%m/%d/%H").
* the data *format* (e.g., newline-separated json)
* a *filter* over the data that's implicitly applied to all queries (e.g.,
  "audit records only")

Now define one or more **queries**, which is basically the same as today,
including:

* a *data source*
* an optional *filter*
* an optional *start and end time*
* an optional *breakdown* (again, just like today)

Finally, we can define one or more **windows** (name subject to change), which
include:

* one or more queries having the same start and end time and timestamp
* optional "join" or other postprocessing operations

## Examples

Scan muskie logs:

    $ dn datasource-add --format=json --backend=manta
	--filter='{ "eq": [ "audit", true ] }'
        --path=/poseidon/stor/logs/muskie --time-format=%Y/%m/%d/%H
	muskie_logs

    $ dn datasource-list
    NAME          LOCATION
    muskie_logs   manta://us-east.manta.joyent.com/poseidon/stor/logs/muskie

Count requests in all muskie logs (full raw data scan):

    $ dn scan muskie_logs
    VALUE
    12345678

Count "GET" requests in logs for June 12, grouped by billable operation (still a
raw scan):

    $ dn scan --after=2014-06-12 --before=2014-06-13
         --filter='{ "eq": [ "req.method", "GET" ] }'
         --breakdowns=operation
    OPERATION            VALUE
    get100                7327
    getjoberrors           807
    getjobfailures         204
    listjobs               426

Instead of doing a raw scan, let's configure these as queries.  First, the query
with no arguments that just counts all records:

    $ dn query-add muskie_logs

Now, the more specific one:

    $ dn query-add --name=getsByOp --filter='{ "eq": [ "req.method", "GET" ] }'
         --breakdowns=operation muskie_logs

Show what we've configured:

    $ dn query-list
    DATASOURCE     QUERY       STATUS
    muskie_logs    total       needs backfill
    muskie_logs    getsByOp    needs backfill

    $ dn query-list -v
    DATASOURCE     QUERY       STATUS
    muskie_logs    total       needs backfill
    muskie_logs    getsByOp    needs backfill
        filter:     req.method == "GET"
        breakdowns: operation

When we're happy with what we've configured, we can kick off a job to backfill
these queries:

    $ dn query-backfill --datasource=muskie_logs     # takes a while

At least at first, I imagine that operation would trigger a rebuild of all
indexes for all queries, and "ready" status is all-or-nothing: either we've
built them all, or we haven't.  We could make the operation only update the
existing queries instead, and we could provide more fine-grained visibility into
query backfilling, but I'd punt on this for a while.

Anyway, when that finishes, the status would be:

    $ dn query-list
    DATASOURCE     QUERY       STATUS
    muskie_logs    total       up to date
    muskie_logs    getsByOp    up to date

Now you can fetch the results of these queries:

    $ dn query-fetch muskie_logs total
    VALUE
    1234567

    $ dn query-fetch --before=2014-06-12 --after=2014-06-13 muskie_logs getsByOp
    OPERATION            VALUE
    get100                7327
    getjoberrors           807
    getjobfailures         204
    listjobs               426


## Comparison to today

By comparison, the way you do this stuff today is that you specify everything
you care about every time.  There's no state.  So it's:

    $ dn scan-manta --filter='{ "eq": [ "audit", true ] }'
        --time-format=%Y/%m/%d/%H 
	/poseidon/stor/logs/muskie 

    $ dn scan-manta --time-format=%Y/%m/%d/%H 
        --after=2014-06-12 --before=2014-06-13
        --filter='{ "and": [ { "eq": [ "audit", true ] }, { "eq": [ "req.method", "GET" ] } ] }'
         --breakdowns=operation
	/poseidon/stor/logs/muskie 

    $ dn index-manta --time-format=%Y/%m/%d/%H 
        --filter='{ "eq": [ "audit", true ] }'
        --columns=operation,req.method
        /poseidon/stor/logs/muskie /path/to/my/index
    ...

    $ dn query-mjob /path/to/my/index

    $ dn query-mjob --filter={ "eq": [ "req.method", "GET" ] } 
        --breakdowns=operation /path/to/my/index


## Windows

Windows (again, the name is tentative) are basically a view on one or more
queries or windows, with optional post-processing.  The idea is that each graph
in a dashboard would be its own window.  Windows are where we'd implement
postprocessing, including:

* combining results of queries over multiple data sources (e.g., plotting muskie
  requests against moray operations)
* simple joins: replacing one set of ids for another (e.g., map user uuids to
  login names)
* top-N filtering
* simple transformations on data, like:
    * map "<400" to "client error", >=500 to "server error", and everything else
      to "success"
    * compute "error rate" as "requests with status >= 500" divided by total
      requests

As a straw man, here's a window that takes an existing query showing
requestsByUserUuid and maps users to logins using a file-based database:

    $ dn window-create requestsByLogin
    $ dn window-add requestsByLogin map --field=user --file=my_user_db
          muskie_logs/requestsByUserUuid

    $ dn query-fetch requestsByUserUuid
    USER                                 COUNT
    b9429272-1e5f-11e4-8865-374e1b509157   157
    ...

    $ dn window-fetch requestsByLogin
    USER                                 COUNT
    dap                                    157
    ...

Here's a window showing error rate.  First, create a window showing just the
number of errors:

    $ dn window-create errorCount
    $ dn window-add errorCount --filter '{ "ge": [ "res.statusCode", 500 ] }'
	muskie_logs/requests
    $ dn window-fetch errorCount
    errorCount
      123

Now create a window that divides this by the total number of requests:

    $ dn window-create errorCount
    $ dn window-add errorRate divide errorCount muskie_logs/requests
    $ dn window-fetch errorRate
    errorRate
    0.012


## Open questions

* Will we ever want to expose indexes to users and allow them to say which
  queries are provided by which indexes?  (Answer: we can always add this
  later and make today's behavior just the "default" index.)
* Relatedly: how do we expose index parameters to users (e.g., hourly vs. daily)
* Would like both local and Manta-based options for storing state (list of
  data sources, queries, etc.)
