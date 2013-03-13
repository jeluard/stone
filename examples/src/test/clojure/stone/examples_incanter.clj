(ns stone.examples-incanter
  (:use [stone.core :as st]
        [stone.incanter.core :as st-in]
        [incanter.core :as in]
        [incanter.charts :as in-c])
  (:import
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.storage.memory MemoryStorage)))

(def dispatcher (SequentialDispatcher.))
(def storage (MemoryStorage. 1000))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list storage (fn [a b] (println (str "Got consolidates " b)))))))

(def wts (st/create-windowed-ts "windowed-timeseries" windows dispatcher))

(def now (System/currentTimeMillis))
(st/publish wts now 1)
(st/publish wts (+ now 1) 2)
(st/publish wts (+ now 2) 3)

(def data (st-in/fetch-dataset storage))

;;(in/view (in-c/histogram data))

(st/close wts)