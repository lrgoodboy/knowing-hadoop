(ns knowing-hadoop.core
  (:require [knowing-hadoop.database :as database]
            [clj-time.core]
            [clojure.tools.logging :as logging]
            [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule]
            [clojure-hadoop.filesystem :as fs])
  (:use [clojure-hadoop.job :only [run]])
  (:gen-class))

(defn process-file [filename date]
  (with-open [rdr (fs/buffered-reader filename)]
    (doall (for [line (line-seq rdr)
                 :let [matches (re-matches #"^([0-9]+)\t([0-9]+)$" line)]
                 :when matches]
             {"f_ds_id" (read-string (nth matches 1))
              "f_data" (read-string (nth matches 2))
              "f_time" (util/unparse-ymd date)}))))

(defn process-files [filenames date]
  (database/add-chartdata-daily!
    (apply concat (for [filename filenames]
                    (process-file filename date)))))

(defn get-filenames [directory]
  (for [file-status (.listStatus (fs/filesystem directory) (fs/make-path directory))
        :let [path (.getPath file-status)]
        :when (.. path getName (startsWith "part"))]
    (.. path toUri getPath)))

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
      (util/parse-ymd date-str)
      (catch Exception e
        (logging/error "Invalid date: " date-str)
        (System/exit 1)))
    (util/yesterday)))

(def job-params {"mapred.job.reuse.jvm.num.tasks" "-1"
                 "mapred.reduce.tasks" (util/get-config :hdfs :reduce-tasks)})

(defn -main [& args]

  (let [date (parse-date (first args))
        date-str (util/unparse-ymd date)
        accesslog-path (parse-accesslog-path (util/get-config :hdfs :accesslog-input-path) date)
        soj-path (parse-soj-path (util/get-config :hdfs :soj-input-path) date)
        tmp-path (str "/tmp/knowing-hadoop/ts-" (util/millitime))]

    (alter-var-root #'job-params assoc "custom-date" date-str)

    (logging/info "Date:" date-str)
    (logging/info "Processing access log in path:" accesslog-path)
    (run {:name "knowing-hadoop.accesslog"
          :map "knowing-hadoop.accesslog/mapper"
          :map-setup "knowing-hadoop.accesslog/mapper-setup"
          :map-cleanup "knowing-hadoop.accesslog/mapper-cleanup"
          :map-reader "clojure-hadoop.wrap/int-string-map-reader"
          :reduce "knowing-hadoop.accesslog/reducer"
          :reduce-setup "knowing-hadoop.accesslog/reducer-setup"
          :input-format "text"
          :output-format "text"
          :compress-output "false"
          :replace "true"
          :input accesslog-path
          :output (str tmp-path "/access_log")
          :params "knowing-hadoop.core/job-params"})
    (persist-data (str tmp-path "/access_log") date)

    (logging/info "Processing soj in path:" soj-path)
    (run {:name "knowing-hadoop.soj"
          :map "knowing-hadoop.soj/mapper"
          :map-setup "knowing-hadoop.soj/mapper-setup"
          :map-cleanup "knowing-hadoop.soj/mapper-cleanup"
          :map-reader "clojure-hadoop.wrap/int-string-map-reader"
          :reduce "knowing-hadoop.soj/reducer"
          :reduce-setup "knowing-hadoop.soj/reducer-setup"
          :input-format (util/get-config :hdfs :soj-input-format)
          :output-format "text"
          :compress-output "false"
          :replace "true"
          :input soj-path
          :output (str tmp-path "/soj")
          :params "knowing-hadoop.core/job-params"})
    (persist-data (str tmp-path "/soj") date)

    (logging/info "Deleting temporary path:" tmp-path)
    (fs/delete tmp-path)))
