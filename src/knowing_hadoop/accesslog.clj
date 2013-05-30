(ns knowing-hadoop.accesslog
  (:require [clojure-hadoop.job]
            [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule]))

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
  (if-let [n (util/parse-double s)]
    (format "%.3f" (/ n 1000.0))
    "0"))

(defn parse-log [log]
  (let [arr (clojure.string/split log #"\t")]
    {"request_time" (divide-1k (get arr 4))
     "upstream_response_time" (divide-1k (get arr 5))
     "remote_addr" (get arr 6)
     "request_length" nil
     "upstream_addr" (get arr 7)
     "time_local" (str (get arr 2) "/"
                       (get-month-name (get arr 20)) "/"
                       (get arr 1) ":"
                       (get arr 0) ":"
                       (get arr 3) ":"
                       (get arr 19) " "
                       "+0800")
     "host" (get arr 8)
     "request_method" (get arr 9)
     "request_url" (get arr 10)
     "request_protocol" nil
     "status" (get arr 11)
     "bytes_send" (get arr 12)
     "http_referer" (get arr 13)
     "http_user_agent" (get arr 14)
     "gzip_ratio" (get arr 15)
     "http_x_forwarded_for" (get arr 16)
     "server_addr" (get arr 17)
     "cookie_aQQ_ajkguid" (get arr 18)}))

(defn mapper [key value]
  (util/time-it "accesslog.mapper"
                (when-let [log (parse-log value)]
                  (rule/filter-log "access_log" log))
                9999))

(defn mapper-setup [context]
  (rule/bind-date context)
  (rule/clear-result))

(defn mapper-cleanup [context]
  (util/time-it "accesslog.mapper-cleanup"
                (rule/write-result context)))

(defn reducer [key values-fn]
  (util/time-it (str "accesslog.reducer[" key "]")
                [[key (rule/collect-result "access_log" key (values-fn))]]))

(defn reducer-setup [context]
  (rule/bind-date context))
