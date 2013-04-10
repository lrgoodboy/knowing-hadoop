(ns knowing-hadoop.accesslog-test
  (:use clojure.test
        knowing-hadoop.accesslog))

(def demo-log (str "8.297 8.297 183.129.158.242 - 10.10.3.50:20080  [26/Jul/2012:15:35:17 +0800] "
                   "hangzhou.anjuke.com \"GET /ajax/checklogin/?r=0.728722894375067 HTTP/1.1\" 200 647 "
                   "\"http://hangzhou.anjuke.com/\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SV1; .NET CLR 2.0.50727; 360SE)\" "
                   "\"-\" \"-\" - \"114.80.230.198 8372DF44-7245-8356-0891-4B001FF015B4\""))

(deftest parse-log-test
  (let [log (parse-log demo-log)]
    (is (map? log))
    (is (= "8.297" (get log "request_time")))
    (is (= "-" (get log "gzip_ratio")))))
