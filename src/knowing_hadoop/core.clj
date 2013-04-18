(ns knowing-hadoop.core
  (:require [knowing-hadoop.database :as database])
  (:use [clojure-hadoop.job :only [run]]))

(defn process-file [filename]
  (with-open [rdr (clojure.java.io/reader filename)]
    (doall (for [line (line-seq rdr)
                 :let [matches (re-matches #"^([0-9]+)\t([0-9]+)$" line)]
                 :when matches]
             {"f_ds_id" (read-string (nth matches 1))
              "f_data" (read-string (nth matches 2))
              "f_time" "2013-04-16"}))))

(defn process-files [filenames]
  (database/add-chartdata-daily!
    (apply concat (for [filename filenames]
                    (process-file filename)))))

(defn get-filenames-single [directory]
  (for [file (file-seq (clojure.java.io/file directory))
        :when (.. file getName (startsWith "part"))]
    (.getPath file)))

(defn get-filenames [directories]
  (apply concat (for [directory directories]
                  (get-filenames-single directory))))

(defn -main [& args]

  (run {:map "knowing-hadoop.accesslog/mapper"
        :map-reader "clojure-hadoop.wrap/int-string-map-reader"
        :reduce "knowing-hadoop.accesslog/reducer"
        :input-format "text"
        :output-format "text"
        :compress-output "false"
        :replace "true"
        :input "test_logs/access_log"
        :output "test_result/access_log"})

  (run {:map "knowing-hadoop.soj/mapper"
        :map-reader "clojure-hadoop.wrap/int-string-map-reader"
        :reduce "knowing-hadoop.soj/reducer"
        :input-format "text"
        :output-format "text"
        :compress-output "false"
        :replace "true"
        :input "test_logs/soj"
        :output "test_result/soj"})

  (let [filenames (get-filenames ["test_result/access_log"
                                  "test_result/soj"])
        result (process-files filenames)]
    (println (count result) "rows inserted.")))
