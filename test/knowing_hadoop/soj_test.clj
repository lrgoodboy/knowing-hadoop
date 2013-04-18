(ns knowing-hadoop.soj-test
  (:require [knowing-hadoop.util :as util])
  (:use clojure.test
        knowing-hadoop.soj))

(def demo-log
  (str "pagename:"
       (util/json-encode
         {"stamp" "-"
          "cstamp" "-"
          "site" "anjuke"
          "url" "-"
          "referer" "-"
          "p" "-"
          "pn" "-"
          "rfpn" "-"
          "guid" "-"
          "uguid" "-"
          "sessid" "-"
          "cip" "-"
          "cid" "-"
          "agent" "-"
          "cstream" "-"
          "lui" "-"})))


(deftest parse-log-test
  (let [log (parse-log demo-log)]
    (is (= "anjuke" (get log "site")))))
