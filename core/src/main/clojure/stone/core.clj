(ns stone.core
  (:import (com.github.jeluard.guayaba.base Pair)
           (com.github.jeluard.stone.api ConsolidationListener Listener Reader TimeSeries Window WindowedTimeSeries)
           (com.github.jeluard.stone.helper Loggers Storages)
           (com.github.jeluard.stone.pattern Database Poller)
           (com.github.jeluard.stone.spi Storage)
           (com.google.common.base Function Optional)
           (java.io Closeable)
           (java.util Iterator)))

(set! *warn-on-reflection* true)

(def ^:dynamic *default-granularity* 1)

(defn- wrap-as-listener [fn]
  (reify Listener
    (onPublication [this previousTimestamp currentTimestamp value]
      (fn previousTimestamp currentTimestamp value))))

(defn create-ts
  ([id listeners dispatcher] (create-ts id *default-granularity* listeners dispatcher))
  ([id granularity listeners dispatcher] (create-ts id granularity (Optional/absent) listeners dispatcher))
  ([id granularity latestTimestamp listeners dispatcher] (TimeSeries. id granularity latestTimestamp (map wrap-as-listener listeners) dispatcher)))

(defn beginning [^Reader reader]
  (.orNull (.beginning reader)))

(defn end [^Reader reader]
  (.orNull (.beginning reader)))

(defn lazy-pairs [^Iterator iter]
  (lazy-seq
    (when (.hasNext iter)
      (let [^Pair pair (.next iter)]
        (cons (vector (.first pair) (vec (.second pair))) (lazy-pairs iter))))))

(defn all [^Reader reader]
  (lazy-pairs (.iterator (.all reader))))

(defn during [^Reader reader before after]
  (lazy-pairs (.iterator (.during reader before after))))

(defn- wrap-fn-as-consolidation-listener [fn]
  (reify ConsolidationListener
    (onConsolidation [this timestamp consolidates]
      (fn timestamp (vec consolidates)))))

(defn- wrap-storage-as-consolidation-listener
  ([storage] (wrap-storage-as-consolidation-listener storage Loggers/BASE_LOGGER))
  ([storage logger] (Storages/asConsolidationListener storage logger)))

(defn- wrap-as-consolidation-listener [x]
  (if (instance? Storage x)
    (wrap-storage-as-consolidation-listener x)
    (wrap-fn-as-consolidation-listener x)))

(defn window
  ([size consolidatorTypes] (window size consolidatorTypes '()))
  ([size consolidatorTypes consolidationListeners] (Window. size consolidatorTypes (map wrap-as-consolidation-listener consolidationListeners))))

(defn create-db
  [dispatcher storageFactory]
  (Database. dispatcher storageFactory))

(defn create-windowed-ts
  ([id windows dispatcher] (create-windowed-ts id *default-granularity* windows dispatcher))
  ([id granularity windows dispatcher] (WindowedTimeSeries. id granularity windows dispatcher)))

(defn create-windowed-ts-from-db
  ([^Database db id duration windows] (create-windowed-ts-from-db db id *default-granularity* duration windows))
  ([^Database db id granularity duration windows] (.createOrOpen db id granularity duration (into-array windows))))

(defn publish [^TimeSeries ts timestamp value]
  (.publish ts timestamp value))

(defn readers [^Database db id]
  (.get (.getReaders db id)))

(defn close
  ([^Closeable cl] (.close cl))
  ([^Database db ^String id] (.close db id)))

;;

(defn- wrap-fn-as-function [fn]
  (reify Function
    (apply [this input]
      (fn input))))

(defn- wrap-as-function [fn]
  (if (instance? Function fn)
    fn
    (wrap-fn-as-function fn)))

(defn create-poller
  ([period windows metric-extractor dispatcher storage-factory scheduler-executor-service]
    (create-poller period windows (Poller/defaultIdExtractor) metric-extractor dispatcher storage-factory scheduler-executor-service))
  ([period windows id-extractor metric-extractor dispatcher storage-factory scheduler-executor-service]
    (Poller. period windows (wrap-as-function id-extractor) (wrap-fn-as-function metric-extractor) dispatcher storage-factory scheduler-executor-service)))

(defn enqueue [^Poller poller resource]
  (.enqueue poller resource))

(defn dequeue [^Poller poller resource]
  (.dequeue poller resource))

(defn start [^Poller poller]
  (.start poller))

(defn cancel [^Poller poller]
  (.cancel poller))