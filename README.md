Stone is a timeseries database library focused on simplicity/evolutivity, efficiency and robustness. It does only one thing but well: storing aggregates of values evoluing over time (timeseries).

## Inspiration

Stone is inspired by RRD and OLAP databases.

Stone does not try to compete with stream oriented soution (CEP à la esper or storm/s4), metrics genertion (à la yammer metrics).
It also does not provide any facility to generate graphs.

## Features

* 
* Insertion is not related to current time. Values can be inserted anytime as long as they are inserted in order (strictly monotonically increasing).
* Most logic components can be customized via SPI

## Constraints

* Values are stored as-is (i.e. no associated semantic). Lots of existing libraries already offer counter, gauge, .. features.
* Millisecond granularity. Several data point with same millisecond will be rejected
* Only integer values. If you need to store other types yiu will have to encode them as integers.
* No metadata: just plain values
* No timezone assumptions: timestamp stored as-is

## Usage

```

``

## Performance



## Scalability

## Extensibility

## Similar projects

* http://opentsdb.net/
* https://github.com/dustin/seriesly
* http://tempo-db.com/
* http://luca.ntop.org/tsdb.pdf (see https://svn.ntop.org/svn/ntop/trunk/tsdb/)