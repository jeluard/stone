Stone is a timeseries database library focused on simplicity/evolutivity, efficiency and robustness. It does only one thing but well: storing consolidates of values changing over time (TimeSeries).

## Inspiration

Stone is inspired by RRD and OLAP databases.

Stone does not try to compete with stream oriented soution (CEP à la esper or storm/s4), metrics genertion (à la yammer metrics).
It also does not provide any facility to generate graphs.

## Features

* Insertion is not related to current time. Values can be inserted anytime as long as they are inserted in order (strictly monotonically increasing).
* Most logic components can be customized via SPI

## Usage

```
//Define how published values will be consolidated: every minute using *max* algorithm and kept up to 1 hour.
final Archive archive = new Archive(Arrays.asList(MaxConsolidator.class),
  Arrays.asList(new Window(Duration.standardMinutes(1), Duration.standardHours(1))));

//Create the TimeSeries. A new storage will be created if needed.
final TimeSeries timeSeries = new TimeSeries("pinger", Arrays.asList(archive), new SequentialDispatcher(), new JournalIOStorageFactory());

//Publish some values to the TimeSeries.
timeSeries.publish(System.currentTimeMillis(), 123);
...

//Cleanup resources.
timeSeries.close();

//Read persisted data
final Journal journal = ... //Get a ref to the right journal. You might want to use JournalIOStorageFactory for this.
final Storage storage = new JournalIOStorage(journal);
for (final Pair<Long, int[]> pair : storage.all().values()) {
  System.out.println("Got values <"+Arrays.toString(pair.second)+"> at <"+pair.first+">");
}
```

## Performance

Extra care has been taken to limit CPU usage and object creation in the publish critical path.

## Extensibility

A number of SPI are defined allowing 

## Constraints

* Values are stored as-is (i.e. no associated semantic). Lots of existing libraries already offer counter, gauge, .. features.
* Millisecond granularity. Several data point with same millisecond will be rejected
* Only integer values. If you need to store other types yiu will have to encode them as integers.
* No metadata: just plain values
* No timezone assumptions: timestamp stored as-is

## Similar projects

* http://opentsdb.net/
* https://github.com/dustin/seriesly
* http://tempo-db.com/
* http://luca.ntop.org/tsdb.pdf (see https://svn.ntop.org/svn/ntop/trunk/tsdb/)