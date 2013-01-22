Stone is a timeseries database library focused on simplicity/evolutivity, efficiency and robustness. It does only one thing but well: storing consolidates of values changing over time (TimeSeries).

## Inspiration

Stone is inspired by [RRD](http://oss.oetiker.ch/rrdtool/) and [OLAP](http://en.wikipedia.org/wiki/Online_Analytical_Processing) databases.

Stone does not try to compete with stream oriented soution (CEP à la [esper](http://esper.codehaus.org/) or [storm](http://storm-project.net/)/[s4](http://incubator.apache.org/s4/)) and does not provide any metrics tracking facility (à la [metrics](http://metrics.codahale.com/)).
It also does not provide any facility to generate graphs.

## Features

* Insertion is not related to current time. Values can be inserted anytime as long as they are inserted in order (strictly monotonically increasing).
* Most logic components can be customized via SPI

## Usage

```java

//Create our main database. JournalIO will be used as storage.
final Database database = new Database(new JournalIOStorageFactory());

//Define how published values will be consolidated: every minute using *max* algorithm and kept up to 1 hour.
final Window window = new Window(Duration.standardMinutes(1), Duration.standardHours(1));
final Archive archive = new Archive(Arrays.asList(MaxConsolidator.class),
  Arrays.asList(window));

//Create the TimeSeries. A new storage will be created if needed.
final TimeSeries timeSeries = database.create("pinger", Arrays.asList(archive));

//Publish some values to the TimeSeries.
timeSeries.publish(System.currentTimeMillis(), 123);
...

//You can also hook some logic at consolidation time
final ConsolidationListener consolidationListener = new ConsolidationListener() {
  @Override
  public void onConsolidation(final Window window, final long timestamp, final int[] consolidates) {
    System.out.println("Got "+Arrays.toString(consolidates));
  }
};

//That will be triggered for a specific TimeSeries
final TimeSeries monitoredTimeSeries = database.create("pinger-monitored", Arrays.asList(archive), Arrays.asList(consolidationListener));

//Access underlying persisted data
final Map<Window, Reader> readers = timeSeries.getReaders();
final Reader reader = readers.get(window);

//Browse everything
Iterable<Pair<Long, int[]>> all = reader.all();

//Or what happened during last day (for simplicity timezone concerns are ignored).
Iterable<Pair<Long, int[]>> lastDay = reader.during(new Interval(DateTime.now().minusDays(1), DateTime.now()));

//Cleanup resources.
database.close();
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