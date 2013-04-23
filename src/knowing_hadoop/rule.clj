(ns knowing-hadoop.rule
  (:require [knowing-hadoop.util :as util]
            [clojure.tools.logging :as logging]
            [clj-yaml.core :as yaml]))

(defn get-datasources []
  (let [datasources (yaml/parse-string
                      (slurp (clojure.java.io/resource "datasources.yaml")))]
    (into {} (for [[datasource fields] datasources]
               [(name datasource)
                (into {} (for [[field type] fields]
                           [(name field) (keyword type)]))]))))

(def datasources (delay (get-datasources)))

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
    :nin (not (util/in-array a b))))

(defn filter-matches-inner [filter log-content]
  (let [field (:field filter)
        operator (:operator filter)
        filter-content (:content filter)]
    (case (get-in @datasources [*datasource* field])
      :string (filter-str log-content operator filter-content)
      :numeric (let [log-content-numeric (read-string log-content)]
                 (when (number? log-content-numeric)
                   (filter-num log-content-numeric operator filter-content))))))

(defn filter-matches [filter]
  (when-let [content (get *log* (:field filter))]
    (let [result (filter-matches-inner filter content)]
      (if (:negative filter) (not result) result))))

(defn rule-matches [rule log]
  (binding [*datasource* (:datasource rule) *log* log]
    (when (every? filter-matches (:filters rule))
      (let [rule-type (:rule-type rule)]
        [(:id rule)
         (cond
           (= :count rule-type) nil

           (some #{rule-type} [:unique :average :ninety])
           (get log (:field rule)))]))))

(defn parse-filter [datasource filter]
  (let [field (nth filter 0)
        field-type (get-in @datasources [datasource field])
        operator (keyword (nth filter 1))
        negative (nth filter 2)
        content (nth filter 3)]
    (Filter. field
             (case field-type
               :string (if (#{:equals :contains :startswith :endswith :regex :in} operator)
                         operator
                         (throw (Exception. (str "Invalid operator: " operator))))
               :numeric (if (#{:eq :neq :gt :gte :lt :lte :in :nin} operator)
                          operator
                          (throw (Exception. (str "invalid operator: " operator)))))
             (case field-type
               :string (true? negative)
               :numeric nil)
             (case field-type
               :string (cond
                         (#{:equals :contains :startswith :endswith} operator)
                         content

                         (= :regex operator) (re-pattern content)
                         (= :in operator) (seq content)

                         :else (throw (Exception. (str "Unkown operator: " operator))))

               :numeric (cond
                          (#{:eq :neq :gt :gte :lt :lte} operator)
                          content

                          (#{:in :nin} operator)
                          (seq content)

                          :else (throw (Exception. (str "Unkown operator: " operator))))))))

(defn parse-rule [rule-id rule-info]
  (let [datasource (get rule-info "datasource")
        rule-type (keyword (get rule-info "rule_type"))
        field (get rule-info "field")
        filters (get rule-info "filters")]
    (Rule. rule-id
           (if (get @datasources datasource)
             datasource
             (throw (Exception. (str "Invalid datasource: " datasource))))
           (if (#{:count :unique :average :ninety} rule-type)
             rule-type
             (throw (Exception. (str "Invalid rule-type: " rule-type))))
           (cond
             (= :count rule-type) nil
             (= :unique rule-type) field

             (#{:average :ninety} rule-type)
             (if (= :numeric (get-in @datasources [datasource field]))
               field
               (throw (Exception. (str "Invalid field: " field)))))
           (for [filter filters]
             (parse-filter datasource filter)))))

(defn parse-rules [rule-map]
  (let [rules (for [[rule-id-str rule-info-json] rule-map]
                (let [rule-id (read-string rule-id-str)
                      rule-info (util/json-decode rule-info-json)]
                  (if-not (number? rule-id)
                    (logging/warn (str "Invalid rule-id " rule-id))
                    (if (nil? rule-info)
                      (logging/warn (str "Invalid JSON for rule-id " rule-id))
                      (try
                        (parse-rule rule-id rule-info)
                        (catch Exception e
                          (logging/error "Unable to parse rule-id " rule-id)))))))
        rules-filtered (filter (complement nil?) rules)
        rules-grouped (group-by #(:datasource %) rules-filtered)]
    (into {} (for [[datasource rules] rules-grouped]
               [datasource
                (into {} (for [rule rules]
                           [(:id rule) rule]))]))))

(defn get-rules []
  (let [rule-path (util/get-config :zookeeper :rule-path)
        children (util/zk-get-children rule-path)
        rules (parse-rules children)]
    (if (seq rules)
      (let [output (apply str "Rules loaded - "
                          (for [[k v] rules]
                            (str k ":" (count v) " ")))]
        (logging/info output))
      (logging/info "No rules."))
    rules))

(def rules (delay (get-rules)))

(defn filter-log [datasource log]
  (filter (complement nil?)
          (for [[rule-id rule] (get @rules datasource)]
            (rule-matches rule log))))

(defn filter-number [values]
  (filter number? (map read-string values)))

(defn calc-average [values]
  (/ (reduce + values) (count values)))

(defn calc-ninety [values]
  (let [values-sorted (sort values)]
    (nth values-sorted (-> values-sorted count (* 0.9) dec))))

(defn collect-result-inner [rule values]
  (let [rule-type (:rule-type rule)]
    (case rule-type
      :count (-> values count long)
      :unique (-> values set count long)
      :average (-> values filter-number calc-average (* 1e3) long)
      :ninety (-> values filter-number calc-ninety (* 1e3) long))))

(defn collect-result [datasource rule-id values]
  (collect-result-inner (get-in @rules [datasource rule-id]) values))
