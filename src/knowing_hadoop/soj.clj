(ns knowing-hadoop.soj
  (:require [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule]))

(defn parse-log [log]
  (let [index (.indexOf log ":")]
    (if (not= -1 index)
      (util/json-decode (subs log (inc index))))))

(defn mapper [key value]
  (when-let [log (parse-log value)]
    (rule/filter-log "soj" log)))

(defn reducer [key values-fn]
  [[key (rule/collect-result "soj" key (values-fn))]])
