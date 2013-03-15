(ns stone.example.poller
  (:use [stone.core :as st])
  (:import
    (com.github.jeluard.guayaba.util.concurrent Scheduler)
    (com.github.jeluard.stone.consolidator MaxConsolidator MinConsolidator)
    (com.github.jeluard.stone.dispatcher.sequential SequentialDispatcher)
    (com.github.jeluard.stone.helper Loggers)
    (com.github.jeluard.stone.storage.memory MemoryStorage MemoryStorageFactory)))

(def dispatcher (SequentialDispatcher.))
(def windows (list (window 3 (list MaxConsolidator MinConsolidator)
                             (list (fn [a b] (println (str "Got consolidates " b)))))))

(def sf (MemoryStorageFactory.))
(def es (Scheduler/defaultExecutorService 10 (Loggers/BASE_LOGGER)))
(def poller (st/create-poller 1000 windows (fn [s] (.length s)) dispatcher sf es))

(st/enqueue poller "aaaa")

(st/start poller)

(st/cancel poller)