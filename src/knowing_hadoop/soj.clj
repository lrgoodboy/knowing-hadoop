(ns knowing-hadoop.soj
  (:require [clojure-hadoop.job]
            [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule]))

(defn parse-log [^String log]
  (let [index (.indexOf log ":")]
    (if (not= -1 index)
      (util/json-decode (subs log (inc index))))))

(defn mapper [key value]
  (when-let [log (parse-log value)]
    (rule/filter-log "soj" log)))

(defn mapper-setup [context]
  (rule/bind-date context)
  (rule/clear-result))

(defn mapper-cleanup [context]
  (rule/write-result context))

(defn reducer [key values-fn]
  [[key (rule/collect-result "soj" key (values-fn))]])

(defn reducer-setup [context]
  (rule/bind-date context))
