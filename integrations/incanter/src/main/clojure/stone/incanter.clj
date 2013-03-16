(ns stone.incanter
  (:require [stone.core :as st]
            [incanter.core :as in]
            [incanter.charts :as in-c]))

(def timestamp "timestamp")
(def value "value-")

(defn value-nth [n]
  (str value n))

(defn- col-from-pair [pair]
  (conj (for [i (range (count pair))] (str value (+ i 1))) timestamp))

(defn- pair-as-array [pair]
  (vec (concat (list (nth pair 0))(nth pair 1))))

(defn extract-dataset [reader]
  (let [pairs (st/all reader)]
    (in/dataset (col-from-pair (first pairs)) (map pair-as-array pairs))))