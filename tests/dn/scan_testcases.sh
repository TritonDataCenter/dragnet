#
# scan_testcases.sh: this is a fragment embedded into various test runners to
# check these same test cases in various modes.  This should be included after
# defining the "scan" function which takes filter and breakdown arguments.
#

# Count everything.
scan

# Break down results by operation.
scan -b operation

# Break down results by operation, request method (a nested property), and host.
scan -b operation,req.method,host

# Break down results by caller, which may be null or undefined
scan -b req.caller
scan -b operation,req.caller

# Get a count, filtered on request method
scan -f '{ "eq": [ "req.method", "GET" ] }'

# Break down that result by operation, request method, and host.
scan -f '{ "eq": [ "req.method", "GET" ] }' -b operation,req.method,host

# Get a count, filtered on caller (which may be null or undefined)
scan -f '{ "eq": [ "req.caller", "poseidon" ] }'
scan -f '{ "eq": [ "req.caller", "poseidon" ] }' -b req.caller

# Now try try a quantization by itself
scan -b latency[aggr=quantize]

# Quantization followed by normal fields: no histogram
scan -b latency[aggr=quantize],operation,host

# Ends with quantization: histogram
scan -b host,operation,latency[aggr=quantize]

# Try a linear quantization
scan -b latency[aggr=lquantize\;step=100]
