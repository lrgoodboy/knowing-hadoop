(ns knowing-hadoop.accesslog
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob]
            [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule])
  (:use [clojure-hadoop.job :only [run]]))

;; '$request_time $upstream_response_time $remote_addr $request_length $upstream_addr  [$time_local] '
;; '$host "$request_method $request_url $request_protocol" $status $bytes_send '
;; '"$http_referer" "$http_user_agent" "$gzip_ratio" "$http_x_forwarded_for" - "$server_addr $cookie_aQQ_ajkguid"'
;(def ptrn (re-pattern (str "(.*?) (.*?) (.*?) (.*?) (.*?)  \\[(.*?)\\] "
;                           "(.*?) \"(.*?) (.*?) (.*?)\" (.*?) (.*?) "
;                           "\"(.*?)\" \"(.*?)\" \"(.*?)\" \"(.*?)\" - \"(.*?) (.*?)\"")))

(defn get-month-name [month]
  (case month
    "01" "Jan"
    "02" "Feb"
    "03" "Mar"
    "04" "Apr"
    "05" "May"
    "06" "Jun"
    "07" "Jul"
    "08" "Aug"
    "09" "Sep"
    "10" "Oct"
    "11" "Nov"
    "12" "Dec"
    month))

(defn divide-1k [s]
  (let [n (read-string s)]
    (if (number? n)
      (format "%.3f" (/ n 1000.0))
      s)))

(defn parse-log [log]
  (let [arr (util/split-line log)]
    {"request_time" (divide-1k (nth arr 4))
     "upstream_response_time" (divide-1k (nth arr 5))
     "remote_addr" (nth arr 6)
     "request_length" nil
     "upstream_addr" (nth arr 7)
     "time_local" (str (nth arr 2) "/"
                       (get-month-name (nth arr 20)) "/"
                       (nth arr 1) ":"
                       (nth arr 0) ":"
                       (nth arr 3) ":"
                       (nth arr 19) " "
                       "+0800")
     "host" (nth arr 8)
     "request_method" (nth arr 9)
     "request_url" (nth arr 10)
     "request_protocol" nil
     "status" (nth arr 11)
     "bytes_send" (nth arr 12)
     "http_referer" (nth arr 13)
     "http_user_agent" (nth arr 14)
     "gzip_ratio" (nth arr 15)
     "http_x_forwarded_for" (nth arr 16)
     "server_addr" (nth arr 17)
     "cookie_aQQ_ajkguid" (nth arr 18)}))

(defn mapper [key value]
  (when-let [log (parse-log value)]
    (rule/filter-log "access_log" log)))

(defn reducer [key values-fn]
  [[key (rule/collect-result "access_log" key (values-fn))]])

(defjob/defjob job
  :map mapper
  :map-reader wrap/int-string-map-reader
  :reduce reducer
  :input-format :text
  :output-format :text
  :compress-output false
  :replace true
  :input "test_logs"
  :output "test_result")
