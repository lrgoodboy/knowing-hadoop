(ns knowing-hadoop.rule
  (:require [knowing-hadoop.util :as util]))

(defn get-datasources []
  (let [datasources (util/get-config :datasources)]
    (into {} (for [[datasource fields] datasources]
               [(name datasource)
                (into {} (for [[field type] fields]
                           [(name field) (keyword type)]))]))))

(def datasources (get-datasources))

(declare ^:dynamic *datasource*)
(declare ^:dynamic *log*)

(defrecord Rule
  [id datasource rule-type field filters])

(defrecord Filter
  [field operator negative content])

(defn filter-str [a operator b]
  (case operator
    :str-equals (= a b)))

(defn filter-num [a operator b]
  (case operator
    :num-gt (> a b)))

(defn filter-matches-inner [filter log-content]
  (let [field (:field filter)
        operator (:operator filter)
        filter-content (:content filter)]
    (case (get-in datasources [*datasource* field])
      :string (filter-str log-content operator filter-content)
      :numeric (let [log-content-numeric (read-string log-content)]
                 (when (number? log-content-numeric)
                   (filter-num log-content-numeric operator filter-content))))))

(defn filter-matches [filter]
  (when-let [content (get *log* (:field filter))]
    (let [result (filter-matches-inner filter content)]
      (if (:negative filter) (not result) result))))

(defn rule-matches [rule]
  (if (every? filter-matches (:filters rule))
    [(:id rule)
     (case (:rule-type rule)
       :count nil
       :unique (get *log* (:field rule)))]))

(defn get-rules []
  [(Rule. 1 "access_log" :count nil
          [(Filter. "host" :str-equals true "www.anjuke.com"),
           (Filter. "status" :num-gt nil 199)]),
   (Rule. 2 "access_log" :unique "status"
          [(Filter. "host" :str-equals false "shanghai.anjuke.com")]),
   (Rule. 3 "access_log" :count nil
          [(Filter. "status" :num-gt nil 200)])])

(defn filter-log [datasource log]
  (binding [*datasource* datasource *log* log]
    (doall (filter (complement nil?) (for [rule (get-rules)]
                                       (rule-matches rule))))))
