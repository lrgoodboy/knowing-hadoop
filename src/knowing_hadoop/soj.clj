(ns knowing-hadoop.soj
  (:require [clojure-hadoop.job]
            [knowing-hadoop.util :as util]
            [knowing-hadoop.rule :as rule]))

(defn parse-log [^String log]
  (let [index (.indexOf log ":")]
    (if (not= -1 index)
      (util/json-decode (subs log (inc index))))))

(defn mapper [key value]
  (util/time-it "soj.mapper"
                (when-let [log (parse-log value)]
                  (rule/filter-log "soj" log))
                9999))

(defn mapper-setup [context]
  (rule/bind-date context)
  (rule/clear-result))

(defn mapper-cleanup [context]
  (util/time-it "soj.mapper-cleanup"
                (rule/write-result context)))

(defn reducer [key values-fn]
  (util/time-it "soj.reducer"
                [[key (rule/collect-result "soj" key (values-fn))]]))

(defn reducer-setup [context]
  (rule/bind-date context))
