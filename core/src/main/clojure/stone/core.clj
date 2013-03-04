(ns stone.core
  (:import (com.github.jeluard.guayaba.base Pair)
           (com.github.jeluard.stone.api ConsolidationListener Listener Reader TimeSeries Window WindowedTimeSeries)
           (com.github.jeluard.stone.spi Storage)
           (com.github.jeluard.stone.helper Loggers Storages)
           (com.google.common.base Optional)
           (java.util Iterator)))

(set! *warn-on-reflection* true)

(defn -wrap-as-listener [fn]
  (reify Listener
    (onPublication [this previousTimestamp currentTimestamp value]
      (fn previousTimestamp currentTimestamp value))))

(defn create-ts
  ([id listeners dispatcher] (create-ts id 1 listeners dispatcher))
  ([id granularity listeners dispatcher] (create-ts id granularity (Optional/absent) listeners dispatcher))
  ([id granularity latestTimestamp listeners dispatcher] (TimeSeries. id granularity latestTimestamp (map -wrap-as-listener listeners) dispatcher)))

(defn beginning [^Reader reader]
  (.get (.beginning reader)))

(defn end [^Reader reader]
  (.get (.beginning reader)))

(defn lazy-pairs [^Iterator iter]
  (lazy-seq
    (when (.hasNext iter)
      (let [^Pair pair (.next iter)]
        (cons (vector (.first pair) (vec (.second pair))) (lazy-pairs iter))))))

(defn all [^Reader reader]
  (lazy-pairs (.iterator (.all reader))))

(defn during [^Reader reader before after]
  (lazy-pairs (.iterator (.during reader before after))))

(defn -wrap-fn-as-consolidation-listener [fn]
  (reify ConsolidationListener
    (onConsolidation [this timestamp consolidates]
      (fn timestamp (vec consolidates)))))

(defn -wrap-storage-as-consolidation-listener
  ([storage] (-wrap-storage-as-consolidation-listener storage Loggers/BASE_LOGGER))
  ([storage logger] (Storages/asConsolidationListener storage logger)))

(defn -wrap-as-consolidation-listener [x]
  (if (instance? Storage x)
    (-wrap-storage-as-consolidation-listener x)
    (-wrap-fn-as-consolidation-listener x)))

(defn window
  ([size consolidatorTypes] (window size consolidatorTypes '()))
  ([size consolidatorTypes consolidationListeners] (Window. size consolidatorTypes (map -wrap-as-consolidation-listener consolidationListeners))))

(defn create-windowed-ts
  ([id windows dispatcher] (create-windowed-ts id 1 windows dispatcher))
  ([id granularity windows dispatcher] (WindowedTimeSeries. id granularity windows dispatcher)))

(defn publish [^TimeSeries ts timestamp value]
  (.publish ts timestamp value))

(defn close [^TimeSeries ts]
  (.close ts))