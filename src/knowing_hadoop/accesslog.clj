(ns knowing-hadoop.accesslog
  (:use knowing-hadoop.rule))

; '$request_time $upstream_response_time $remote_addr $request_length $upstream_addr  [$time_local] '
; '$host "$request_method $request_url $request_protocol" $status $bytes_send '
; '"$http_referer" "$http_user_agent" "$gzip_ratio" "$http_x_forwarded_for" - "$server_addr $cookie_aQQ_ajkguid"'
(def ptrn (re-pattern (str "(.*?) (.*?) (.*?) (.*?) (.*?)  \\[(.*?)\\] "
                           "(.*?) \"(.*?) (.*?) (.*?)\" (.*?) (.*?) "
                           "\"(.*?)\" \"(.*?)\" \"(.*?)\" \"(.*?)\" - \"(.*?) (.*?)\"")))

(defn parse-log [log]
  (when-let [matches (re-matches ptrn log)]
    {"request_time" (nth matches 1)
     "upstream_response_time" (nth matches 2)
     "remote_addr" (nth matches 3)
     "request_length" (nth matches 4)
     "upstream_addr" (nth matches 5)
     "time_local" (nth matches 6)
     "host" (nth matches 7)
     "request_method" (nth matches 8)
     "request_url" (nth matches 9)
     "request_protocol" (nth matches 10)
     "status" (nth matches 11)
     "bytes_send" (nth matches 12)
     "http_referer" (nth matches 13)
     "http_user_agent" (nth matches 14)
     "gzip_ratio" (nth matches 15)
     "http_x_forwarded_for" (nth matches 16)
     "server_addr" (nth matches 17)
     "cookie_aQQ_ajkguid" (nth matches 18)}))

(defn mapper [key value]
  (when-let [log (parse-log value)]
    (filter-log "access_log" log)))
