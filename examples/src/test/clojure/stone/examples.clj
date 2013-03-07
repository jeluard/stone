(ns stone.examples
  (:use [stone.core :as st])
  (:import
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.storage.memory MemoryStorage)))

(def dispatcher (SequentialDispatcher.))

(def ts (st/create-ts "timeseries" (list (fn [a b c] (println (str "Got value " c)))) dispatcher))

(st/publish ts 123 1)

(st/close ts)

;;

(def storage (MemoryStorage. 1000))

(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list storage (fn [a b] (println (str "Got consolidates " b)))))))

(def wts (st/create-windowed-ts "windowed-timeseries" windows dispatcher))

(def now (System/currentTimeMillis))
(st/publish wts now 1)
(st/publish wts (+ now 1) 2)
(st/publish wts (+ now 2) 3)

(println (take 2 (st/all storage)))
(println (str "First timestamp stored " (st/end storage)))
(println (str "Latest timestamp stored " (st/end storage)))

(st/close wts)