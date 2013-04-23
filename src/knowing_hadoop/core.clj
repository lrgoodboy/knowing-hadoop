(ns knowing-hadoop.core
  (:require [knowing-hadoop.database :as database]
            [clj-time.core]
            [clj-time.format]
            [clj-time.local]
            [clojure.tools.logging :as logging]
            [knowing-hadoop.util :as util]
            [clojure-hadoop.filesystem])
  (:use [clojure-hadoop.job :only [run]]))

(defn process-file [filename date]
  (with-open [rdr (clojure.java.io/reader filename)]
    (doall (for [line (line-seq rdr)
                 :let [matches (re-matches #"^([0-9]+)\t([0-9]+)$" line)]
                 :when matches]
             {"f_ds_id" (read-string (nth matches 1))
              "f_data" (read-string (nth matches 2))
              "f_time" (clj-time.format/unparse-local (:date clj-time.format/formatters) date)}))))

(defn process-files [filenames date]
  (database/add-chartdata-daily!
    (apply concat (for [filename filenames]
                    (process-file filename date)))))

(defn get-filenames [directory]
  (for [file (file-seq (clojure.java.io/file directory))
        :when (.. file getName (startsWith "part"))]
    (.getPath file)))

(defn persist-data [directory date]
  (let [filenames (get-filenames directory)
        result (process-files filenames date)]
    (logging/info (count result) "rows inserted.")))

(defn replace-ymd [input date]
  (-> input
    (clojure.string/replace #"\{yyyy\}" (str (clj-time.core/year date)))
    (clojure.string/replace #"\{MM\}" (format "%02d" (clj-time.core/month date)))
    (clojure.string/replace #"\{dd\}" (format "%02d" (clj-time.core/day date)))))

(defn parse-accesslog-path [input date]
  (if (not= -1 (.indexOf input "{"))
    (clojure.string/join "," (let [input (replace-ymd input date)]
                               (for [h (range 24)]
                                 (clojure.string/replace input #"\{HH\}" (format "%02d" h)))))
    input))

(defn parse-soj-path [input date]
  (if (not= -1 (.indexOf input "{"))
    (replace-ymd input date)
    input))

(defn parse-date [date-str]
  (if-not (clojure.string/blank? date-str)
    (try
      (clj-time.format/parse-local-date (:date clj-time.format/formatters) date-str)
      (catch Exception e
        (logging/error "Invalid date: " date-str)
        (System/exit 1)))
    (clj-time.core/minus (clj-time.core/today) (clj-time.core/days 1))))

(defn -main [& args]

  (let [date (parse-date (first args))
        accesslog-path (parse-accesslog-path (util/get-config :hdfs :accesslog-input-path) date)
        soj-path (parse-soj-path (util/get-config :hdfs :soj-input-path) date)
        tmp-path (str "/tmp/knowing-hadoop/ts-" (util/millitime))]

    (logging/info "Date:" (clj-time.format/unparse-local (:date clj-time.format/formatters) date))
    (logging/info "Processing access log in path:" accesslog-path)
    (run {:map "knowing-hadoop.accesslog/mapper"
          :map-reader "clojure-hadoop.wrap/int-string-map-reader"
          :reduce "knowing-hadoop.accesslog/reducer"
          :input-format "text"
          :output-format "text"
          :compress-output "false"
          :replace "true"
          :input accesslog-path
          :output (str tmp-path "/access_log")})
    (persist-data (str tmp-path "/access_log") date)

    (logging/info "Processing soj in path:" soj-path)
    (run {:map "knowing-hadoop.soj/mapper"
          :map-reader "clojure-hadoop.wrap/int-string-map-reader"
          :reduce "knowing-hadoop.soj/reducer"
          :input-format (util/get-config :hdfs :soj-input-format)
          :output-format "text"
          :compress-output "false"
          :replace "true"
          :input soj-path
          :output (str tmp-path "/soj")})
    (persist-data (str tmp-path "/soj") date)

    (logging/info "Deleting temporary path:" tmp-path)
    (clojure-hadoop.filesystem/delete tmp-path)))
