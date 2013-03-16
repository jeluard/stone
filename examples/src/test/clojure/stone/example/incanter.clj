(ns stone.example.incanter
  (:use [stone.core :as st]
        [stone.incanter :as st-in])
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

(dotimes [i 1000]
  (st/publish wts (+ now i) (rand-int 100)))

;;(st-in/histogram storage 1)
;;(st-in/time-series-plot storage 1)

;;(view (time-series-plot :timestamp :value-1 :data data))

(st/close wts)