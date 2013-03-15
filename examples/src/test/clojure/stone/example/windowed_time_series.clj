(ns stone.example.windowed-time-series
  (:use [stone.core :as st])
  (:import
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.storage.memory MemoryStorage MemoryStorageFactory)))

(def dispatcher (SequentialDispatcher.))

(def storage (MemoryStorage. 1000))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list storage (fn [a b] (println (str "Got consolidates " b)))))))

(def wts (st/create-windowed-ts "windowed-timeseries" windows dispatcher))

(def now (System/currentTimeMillis))
(st/publish wts now 1)
(st/publish wts (+ now 1) 2)
(st/publish wts (+ now 2) 3)

(st/all storage)
(st/end storage)
(st/end storage)

(st/close wts)