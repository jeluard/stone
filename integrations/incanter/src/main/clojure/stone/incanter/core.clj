(ns stone.incanter.core
  (:require [stone.core :as st]
            [incanter.core :as in]))

(defn fetch-dataset [reader]
  (in/to-dataset (map (fn [x] (vec (concat (list (nth x 0))(nth x 1)))) (st/all reader))))