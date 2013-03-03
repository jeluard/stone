(ns stone.core
  (:import (com.github.jeluard.stone.api Listener TimeSeries)
           (com.google.common.base Optional)))

(set! *warn-on-reflection* true)

(defn -wrap-fn-as-listener [fn]
  (reify Listener
    (onPublication [this previousTimestamp currentTimestamp value]
      (fn previousTimestamp currentTimestamp value))))

(defn create-ts
  ([id listeners dispatcher] (create-ts id 1 listeners dispatcher))
  ([id granularity listeners dispatcher] (create-ts id granularity (Optional/absent) listeners dispatcher))
  ([id granularity latestTimestamp listeners dispatcher] (TimeSeries. id granularity latestTimestamp (map -wrap-fn-as-listener listeners) dispatcher)))

(defn publish [^TimeSeries ts timestamp value]
  (.publish ts timestamp value))

(defn close [^TimeSeries ts]
  (.close ts))