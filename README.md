Stone is a [time series database](http://en.wikipedia.org/wiki/Time_series_database) library focused on simplicity, efficiency and robustness. It does only one thing but well: storing values changing over time.

Contrary to most other timeseries database a consolidation process pre-calculates what will be stored at publication time (inspired from [RRD](http://oss.oetiker.ch/rrdtool/) and [OLAP](http://en.wikipedia.org/wiki/Online_Analytical_Processing) databases).
This greatly reduce the amount of data to store and remove the processing phase at read time.

![CI status](https://travis-ci.org/jeluard/stone.png?branch=master)

## Principles

```stone``` is built around key principles:

* ```Simplicity``` API is has simple as it can be. Advanced features are built on top of simple abstractions.
* ```Robustness``` built to run for months 
* ```Performance``` low GC impact and optimised [streaming algorithms](http://en.wikipedia.org/wiki/Streaming_algorithm)
* ```Extensibility```various [SPI](http://en.wikipedia.org/wiki/Service_provider_interface) offer clean extension points (see provided [implementations](implementations))

## Getting started

### Dependencies

All dependencies are available in maven central.

With maven:

```xml
<dependency>
  <groupId>com.github.jeluard.stone</groupId>
  <artifactId>stone-core</artifactId>
  <version>0.8-SNAPSHOT</version>
</dependency>
```

With leiningen:

```[com.github.jeluard.stone/stone-core "0.8-SNAPSHOT"]```

Dependending on the components you choose to use you will need to include some other jars.

### Core API

#### Time series

`Time series` is the lowest level abstration. `timestamp/value` pairs can be published to a time series and if valid pushed to every registered `listeners`.
A `timestamp` is valid if strictly greater than previously accepted one modulo specified `granularity`.

In Java:

```java
//Create a TimeSeries. Each data published will be passed to all provided Listener.
final TimeSeries timeSeries = new TimeSeries("timeseries", 1, Arrays.asList(new Listener() {
  //Will be called for each value published
  public void onPublication(long previousTimestamp, long currentTimestamp, int value) {
    System.out.println("Received value"+value);
  }
}),new SequentialDispatcher());

//Publish some values to the TimeSeries.
timeSeries.publish(System.currentTimeMillis(), 123);
...

//Cleanup resources.
timeSeries.close();
```

In clojure:

```clojure
(def dispatcher (SequentialDispatcher.))

(def ts (create-ts "timeseries" (list (fn [a b c] (println (str "Got value " c)))) dispatcher))

(publish ts 123 1)

(close ts)
```

#### Windowed time series

Windowed `time series` build on top of time series and introduce the `window` concept. While in the same window every accepted `timestamp/value` pair is pushed to specified `consolidators`.
When the `window` threshold is crossed (a window holds `size` consecutive timestamps) `consolidates` are pushed to specified `consolidation listeners`.

In java:

```java
//You can also create windowed TimeSeries.
//Data will then be consolidated each time window boundaries are crossed using Consolidators
//and passed to some ConsolidationListeners.

final Window window = Window.of(10).listenedBy(new ConsolidationListener(){
  public void onConsolidation(long timestamp, int[] consolidates) {
    System.out.println("Received consolidates"+Arrays.toString(consolidates));
  }
}).consolidatedBy(MinConsolidator.class, MaxConsolidator.class);
final WindowedTimeSeries windowedTimeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new SequentialDispatcher());

final long now = System.currentTimeMillis();

windowedTimeSeries.publish(now, 123);
windowedTimeSeries.publish(now+10, 234);

windowedTimeSeries.close();

//Storages can be used to provide persistency

final Storage storage = new MemoryStorage(1000);
final Window window = Window.of(10).listenedBy(Storages.asConsolidationListener(storage, Logger.getAnonymousLogger())).consolidatedBy(MaxConsolidator.class);
final WindowedTimeSeries windowedTimeSeries = new WindowedTimeSeries("id", 1, Arrays.asList(window), new SequentialDispatcher());
...
final Iterable<Pair<Long, int[]>> all = storage.all();
final Iterable<Pair<Long, int[]>> subset = storage.during(now, now+5);
```

In clojure:

```clojure
(def dispatcher (SequentialDispatcher.))

(def storage (MemoryStorage. 1000))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list storage (fn [timestamp consolidates] (println (str "Got consolidates " consolidates)))))))

(def wts (create-windowed-ts "windowed-timeseries" windows dispatcher))

(publish wts now 1)
(publish wts (+ 2 now) 2)

(println (take 1 (all storage)))

(close wts)
```

### Patterns

On top of basic API usage higher level patterns facilitates common usages.

#### Database

A `database` ease creation of `windowed time series` sharing `dispatcher` and `storage factory`.
Each time series created will have a `storage` instance created per `id/window` couple.

In java:

```java
final Database database = new Database(new SequentialDispatcher(), new MemoryStorageFactory());

final TimeSeries timeSeries = database.createOrOpen("id", 1000, Window.of(10).consolidatedBy(MaxConsolidator.class));
timeSeries.publish(System.currentTimeMillis(), 1);

database.close();
```

In clojure:

```clojure
(def db (create-db (SequentialDispatcher.) (MemoryStorageFactory.)))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list storage (fn [timestamp consolidates] (println (str "Got consolidates " consolidates)))))))

(def ts-db (create-windowed-ts-from-db db "timeseries" 1000 windows))

(publish ts-db now 1)

(close db)
```

#### Poller

A poller helps keeping regularly track of a unique metric for a collection of similar resources.

In java:

```java
final Poller<String> poller = new Poller<String>(1000, windows, Poller.<String>defaultIdExtractor(), new Function<String, Future<Integer>>() {
  @Override
  public Future<Integer> apply(final String input) {
    return Futures.immediateFuture(input.length());
  }
}, new SequentialDispatcher(), new MemoryStorageFactory(), Scheduler.defaultExecutorService(10, Loggers.BASE_LOGGER));

poller.add("aaaa");
poller.add("bbbbb");
poller.add("ccc");

poller.start();

pollers.remove("ccc");

poller.cancel();
```

In clojure:

```clojure
(def es (Scheduler/defaultExecutorService 10 (Loggers/BASE_LOGGER)))
(def poller (create-poller 1000 windows (fn [s] (.length s)) dispatcher sf es))

(st/enqueue poller "aaaa")

(st/start poller)

(st/cancel poller)
```

More [examples](examples/src/test) explore advanced features.

## License

Released under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).