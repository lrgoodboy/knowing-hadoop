(ns knowing-hadoop.core
  (:require [knowing-hadoop.accesslog :as alog])
  (:use [clojure-hadoop.job :only [run]]))

(defn -main [& args]
  (run alog/job)
  (println (slurp "test_result/part-r-00000")))
