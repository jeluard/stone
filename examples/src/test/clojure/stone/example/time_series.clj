(ns stone.example.time-series
  (:use [stone.core :as st])
  (:import
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)))

(def dispatcher (SequentialDispatcher.))

(def ts (st/create-ts "timeseries" (list (fn [a b c] (println (str "Got value " c)))) dispatcher))

(st/publish ts 123 1)

(st/close ts)