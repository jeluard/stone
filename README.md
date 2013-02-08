![CI status](https://secure.travis-ci.org/jeluard/stone.png)

Stone is a [timeseries database](http://en.wikipedia.org/wiki/Time_series_database) library focused on simplicity, efficiency and robustness. It does only one thing but well: storing consolidates of values changing over time.

Contrary to most other timeseries database a consolidation process pre-calculates what will be stored at publication time (inspired from [RRD](http://oss.oetiker.ch/rrdtool/) and [OLAP](http://en.wikipedia.org/wiki/Online_Analytical_Processing) databases).
This greatly reduce the amount of data to store and remove the processing phase at read time. It also implies you must know in advance your consolidation logic.

## Getting started

Necessary jars are available in maven central:

```xml
<dependency>
  <groupId>com.github.jeluard.stone</groupId>
  <artifactId>stone-core</artifactId>
  <version>0.8-SNAPSHOT</version>
</dependency>
```
Dependending on the components you choose to use you will need to include some other jars.

```java
//Define how published values will be consolidated: every minute using *max* algorithm and kept up to 1 hour.
final Window window = Window.of(Duration.standardMinutes(1)).persistedDuring(Duration.standardHours(1)).consolidatedBy(MaxConsolidator.class);

//Create the TimeSeries. A new storage will be created if needed.
final TimeSeries timeSeries = new TimeSeries("pinger", Duration.millis(1), new Window[]{window}, new SequentialDispatcher(), new MemoryStorageFactory());

//Publish some values to the TimeSeries.
timeSeries.publish(System.currentTimeMillis(), 123);
...

//Access underlying persisted data
final Map<Window, ? extends Reader> readers = timeSeries.getReaders();
final Reader reader = readers.get(window);

//Browse everything
final Iterable<Pair<Long, int[]>> all = reader.all();

//Or what happened during last day (for simplicity timezone concerns are ignored).
final Iterable<Pair<Long, int[]>> lastDay = reader.during(new Interval(DateTime.now().minusDays(1), DateTime.now()));

//Cleanup resources.
timeSeries.close();
```

More [examples]() explore advanced features.

---

Released under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).