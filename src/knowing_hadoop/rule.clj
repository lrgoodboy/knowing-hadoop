(ns knowing-hadoop.rule
  (:require [knowing-hadoop.util :as util]
            [clojure.tools.logging :as logging]
            [clj-yaml.core :as yaml]
            [clj-time.format])
  (:import [org.apache.hadoop.io Text]
           [org.apache.hadoop.mapreduce MapContext]))

(defn get-datasources []
  (let [datasources (yaml/parse-string
                      (slurp (clojure.java.io/resource "datasources.yaml")))]
    (into {} (for [[datasource fields] datasources]
               [(name datasource)
                (into {} (for [[field type] fields]
                           [(name field) (keyword type)]))]))))

(def datasources (delay (get-datasources)))

(declare ^:dynamic *date*)

(defrecord Rule
  [id datasource rule-type field filters])

(defrecord Filter
  [field field-type operator negative content])

(defn filter-str [^String a operator b]
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
  (let [operator (:operator filter)
        filter-content (:content filter)]
    (case (:field-type filter)
      :string (filter-str log-content operator filter-content)
      :numeric (when-let [log-content-numeric (util/parse-double log-content)]
                 (filter-num log-content-numeric operator filter-content)))))

(defn filter-matches [log filter]
  (when-let [content (get log (:field filter))]
    (let [result (filter-matches-inner filter content)]
      (if (:negative filter) (not result) result))))

(defn rule-matches [rule log]
  (every? (partial filter-matches log) (:filters rule)))

(defn peak-time-filter [datasource]
  (when (bound? #'*date*)
    (case datasource
      "access_log" [(Filter. "time_local" :string :startswith false
                             (clj-time.format/unparse-local
                               (clj-time.format/formatter-local "dd/MMM/yyyy:10:") *date*))]
      "soj" (letfn [(hour [h] (.. (clj-time.core/local-date-time (clj-time.core/year *date*)
                                                                 (clj-time.core/month *date*)
                                                                 (clj-time.core/day *date*)
                                                                 h)
                                toDateTime getMillis))]
                   [(Filter. "stamp" :numeric :gte nil (hour 10))
                    (Filter. "stamp" :numeric :lt nil (hour 11))]))))

(defn parse-filter [datasource filter]
  (let [field (nth filter 0)
        field-type (get-in @datasources [datasource field])
        operator (keyword (nth filter 1))
        negative (nth filter 2)
        content (nth filter 3)]
    (Filter.
      ; field
      field
      ; field-type
      field-type
      ; operator
      (case field-type
        :string (if (#{:equals :contains :startswith :endswith :regex :in} operator)
                  operator
                  (throw (Exception. (str "Invalid operator: " operator))))
        :numeric (if (#{:eq :neq :gt :gte :lt :lte :in :nin} operator)
                   operator
                   (throw (Exception. (str "invalid operator: " operator)))))
      ; negative
      (case field-type
        :string (true? negative)
        :numeric nil)
      ; content
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
    (when (or (not (bound? #'*date*)) (not= :count rule-type))
      (Rule.
        ; rule-id
        rule-id
        ; datasource
        (if (get @datasources datasource)
          datasource
          (throw (Exception. (str "Invalid datasource: " datasource))))
        ; rule-type
        (if (#{:count :unique :average :ninety} rule-type)
          rule-type
          (throw (Exception. (str "Invalid rule-type: " rule-type))))
        ; field
        (cond
          (= :count rule-type) nil
          (= :unique rule-type) field

          (#{:average :ninety} rule-type)
          (if (= :numeric (get-in @datasources [datasource field]))
            field
            (throw (Exception. (str "Invalid field: " field)))))
        ; filters
        (let [filters (for [filter filters]
                        (parse-filter datasource filter))]
          (if (#{:average :ninety} rule-type)
            (concat (peak-time-filter datasource) filters)
            filters))))))

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

(def result (ref {}))

(defn alter-result [rule log result]
  (let [rule-id (:id rule)
        rule-type (:rule-type rule)
        content (get log (:field rule))]
    (if (contains? result rule-id)
      (cond
        (= :count rule-type)
        (update-in result [rule-id] inc)

        (#{:unique :average :ninety} rule-type)
        (update-in result [rule-id] conj content))
      (assoc result rule-id (case rule-type
                               :count 1
                               :unique #{content}
                               :average [content]
                               :ninety [content])))))

(defn filter-log [datasource log]
  (doseq [[rule-id rule] (get @rules datasource)
          :when (rule-matches rule log)]
    (dosync (alter result (partial alter-result rule log)))))

(defn filter-number [values]
  (for [value values
        :let [data (util/parse-double value)]
        :when data]
    data))

(defn calc-average [values]
  (if (seq values)
    (/ (reduce + values) (count values))
    0))

(defn calc-ninety [values]
  (if-let [values-sorted (seq (sort values))]
    (nth values-sorted (-> values-sorted count (* 0.9) dec))
    0))

(defn collect-result-inner [rule values]
  (let [rule-type (:rule-type rule)]
    (case rule-type
      :count (-> (reduce + values) long)
      :unique (-> values set count long)
      :average (-> values filter-number calc-average (* 1e3) long)
      :ninety (-> values filter-number calc-ninety (* 1e3) long))))

(defn collect-result [datasource rule-id values]
  (collect-result-inner (get-in @rules [datasource rule-id]) values))


;; functions related to map-reduce job

(defn bind-date [context]
  (let [date (util/parse-ymd (.. context getConfiguration (get "custom-date")))]
    (alter-var-root #'*date* (fn [_] date))))

(defn clear-result []
  (dosync (ref-set result {})))

(defn write-result [^MapContext context]
  (binding [*print-dup* true]
    (let [^Text key-text (Text.) ^Text value-text (Text.)
          write-value (fn [value]
                        (.set value-text (pr-str value))
                        (.write context key-text value-text))]
      (doseq [[rule-id rule-result] @result]
        (.set key-text (pr-str rule-id))
        (cond
          (map? rule-result)
          (doseq [[k v] rule-result]
            (write-value [k v]))

          (set? rule-result)
          (doseq [v rule-result]
            (write-value v))

          :else
          (write-value rule-result))))))
