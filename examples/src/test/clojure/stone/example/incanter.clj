(ns stone.example.incanter
  (:use [stone.core :as st]
        [stone.incanter :as st-in])
  (:import
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.storage.memory MemoryStorage)))

(def dispatcher (SequentialDispatcher.))
(def storage (MemoryStorage. 1000))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator) (list storage))))

(def wts (st/create-windowed-ts "windowed-timeseries" windows dispatcher))

(def now (System/currentTimeMillis))

(dotimes [i 1000]
  (st/publish wts (+ now i) (rand-int 100)))

(def st-dataset (extract-dataset storage))

;;Usage:
;;(use 'stone.incanter)
;;(use 'stone.example.incanter)
;;(use '(incanter core charts))
;;
;;(view (with-data st-dataset
;;  (histogram (value-nth 1))))
;;
;;(view (with-data st-dataset
;;  (time-series-plot timestamp (value-nth 1))))

(st/close wts)