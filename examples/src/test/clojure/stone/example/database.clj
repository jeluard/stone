(ns stone.example.database
  (:use [stone.core :as st])
  (:import
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.storage.memory MemoryStorage MemoryStorageFactory)))

(def now (System/currentTimeMillis))
(def dispatcher (SequentialDispatcher.))
(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list (fn [a b] (println (str "Got consolidates " b)))))))

(def sf (MemoryStorageFactory.))
(def db (st/create-db dispatcher sf))

(def ts-db (st/create-windowed-ts-from-db db "timeseries" 1000 windows))

(st/publish ts-db now 1)

(st/close db)