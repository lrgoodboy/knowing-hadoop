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
    :nin (not (util/in-array a b))))

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
                         (some #{operator} [:equals :contains :startswith :endswith])
                         content

                         (= :regex operator) (re-pattern content)
                         (= :in operator) (seq content)

                         :else (throw (Exception. (str "Unkown operator: " (name operator)))))

               :numeric (cond
                          (some #{operator} [:eq :neq :gt :gte :lt :lte])
                          content

                          (some #{operator} [:in :nin])
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
           (cond
             (= :count rule-type) nil
             (= :unique rule-type) field

             (some #{rule-type} [:average :ninety])
             (if (= :numeric (get (get datasources datasource) field))
               field
               (throw (Exception. (str "Invalid field '" field "' for rule-type '" rule-type "'."))))

             :else (throw (Exception. (str "Unkown rule-type: " (name rule-type)))))
           (for [filter filters]
             (parse-filter datasource filter)))))

(defn parse-rules [rule-map]
  (group-by #(:datasource %)
            (filter (complement nil?)
                    (for [[rule-id rule-info-json] rule-map
                          :let [rule-info (util/json-decode rule-info-json)]
                          :when (not (nil? rule-info))]
                      (parse-rule rule-id rule-info)))))

(defn get-rules []
  (let [client (util/zk-connect)
        rule-path (util/get-config :override :rule-path)
        children (util/zk-get-children client rule-path)]
    (parse-rules children)))

(def rules (get-rules))
(if (seq rules)
  (do
    (print "Rules loaded - ")
    (doseq [[k v] rules]
      (print (str k ":" (count v))))
    (newline))
  (println "No rules."))

(defn filter-log [datasource log]
  (filter (complement nil?)
          (for [rule (get rules datasource)]
            (rule-matches rule log))))

(defn collect-result-inner [rule values]
  (let [rule-type (:rule-type rule)]
    (cond
      (= :count rule-type) (count values)
      (= :unique rule-type) (count (set values))

      (some #{rule-type} [:average :ninety])
      (let [values-numeric (filter number? (map read-string values))]
        (case rule-type
          :average (double (/ (reduce + values-numeric) (count values-numeric)))
          :ninety (let [values-sorted (sort values-numeric)]
                    (nth values-sorted (dec (* (count values-sorted) 0.9)))))))))

(defn collect-result [datasource rule-id values]
  (collect-result-inner (get-in rules [datasource rule-id]) values))
