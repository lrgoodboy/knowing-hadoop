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
    :equals (= a b)
    :contains (.contains a b)
    :startswith (.startsWith a b)
    :endswith (.endsWith a b)
    :regex (re-matches b a)
    :in (util/in-array a b)))

(defn filter-num [a operator b]
  (case operator
    :eq (= a b)
    :neq (not= a b)
    :gt (> a b)
    :gte (>= a b)
    :lt (< a b)
    :lte (<= a b)
    :in (util/in-array a b)
    :nin (not (util/in-array))))

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

(defn parse-filter [datasource filter]
  (let [field (nth filter 0)
        operator (keyword (nth filter 1))
        negative (nth filter 2)
        content (nth filter 3)]
    (Filter. field
             operator
             (case (get-in datasources [datasource field])
               :string (true? negative)
               :numeric nil)
             (case (get-in datasources [datasource field])
               :string (cond
                         (some
                           (partial = operator)
                           [:equals :contains :startswith :endswith])
                         content

                         (= :regex operator) (re-pattern content)
                         (= :in operator) (seq content)

                         :else (throw (Exception. (str "Unkown operator: " (name operator)))))

               :numeric (cond
                          (some
                            (partial = operator)
                            [:eq :neq :gt :gte :lt :lte])
                          content

                          (some
                            (partial = operator)
                            [:in :nin])
                          (seq content)

                          :else (throw (Exception. (str "Unkown operator: " (name operator)))))))))

(defn parse-rule [rule-id rule-info]
  (let [datasource (get rule-info "datasource")
        rule-type (keyword (get rule-info "rule_type"))
        field (get rule-info "field")
        filters (get rule-info "filters")]
    (Rule. rule-id
           datasource
           rule-type
           (case rule-type
             :count nil
             :unique field
             :average field
             :ninety field)
           (for [filter filters]
             (parse-filter datasource filter)))))

(defn get-rules []
  (let [client (util/zk-connect)
        rule-path (util/get-config :zookeeper :rule-path)
        children (util/zk-get-children client rule-path)]
    (group-by #(:datasource %)
              (filter (complement nil?)
                      (for [[rule-id rule-info-json] children
                            :let [rule-info (util/json-decode rule-info-json)]
                            :when (not (nil? rule-info))]
                        (parse-rule rule-id rule-info))))))

(def rules (get-rules))

(defn filter-log [datasource log]
  (binding [*datasource* datasource *log* log]
    (doall ; When using binding, immediate evaluation is a must.
      (filter (complement nil?) (for [rule (get rules datasource)]
                                  (rule-matches rule))))))
