(ns knowing-hadoop.core
  (:require [knowing-hadoop.database :as database])
  (:use [clojure-hadoop.job :only [run]]))

(defn process-file [filename]
  (with-open [rdr (clojure.java.io/reader filename)]
    (doall (for [line (line-seq rdr)
                 :let [matches (re-matches #"^([0-9]+)\t([0-9]+)$" line)]
                 :when matches]
             {"f_ds_id" (read-string (first matches))
              "f_data" (read-string (second matches))
              "f_time" "2013-04-16"}))))

(defn process-files [filenames]
  (database/add-chartdata-daily!
    (apply concat (for [filename filenames]
                    (process-file filename)))))

(defn -main [& args]

  (run {:map "knowing-hadoop.accesslog/mapper"
        :map-reader "clojure-hadoop.wrap/int-string-map-reader"
        :reduce "knowing-hadoop.accesslog/reducer"
        :input-format "text"
        :output-format "text"
        :compress-output "false"
        :replace "true"
        :input "test_logs"
        :output "test_result"})

  (let [result (process-files ["test_result/part-r-00000"])]
    (println (count result) "rows inserted.")))
