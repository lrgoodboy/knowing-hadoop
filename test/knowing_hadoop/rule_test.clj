(ns knowing-hadoop.rule-test
  (:require [knowing-hadoop.util :as util]
            [clj-time.core])
  (:use clojure.test
        knowing-hadoop.rule))

(deftest get-datasources-test
  (let [datasources (get-datasources)]
    (is (= :numeric (get-in datasources
                            ["access_log" "upstream_response_time"])))
    (is (= :string (get-in datasources
                           ["soj" "guid"])))))

(deftest get-rules-test
  (is (get-rules)))

(def rules-test
  {1 {"datasource" "access_log"
      "rule_type" "count"
      "field" nil
      "filters" [["host" "equals" true "www.anjuke.com"]]}

   2 {"datasource" "access_log"
      "rule_type" "unique"
      "field" "remote_addr"
      "filters" [["status" "nin" nil [301 302]]
                 ["remote_addr" "regex" nil "[0-9\\.]+"]]}

  3 {"datasource" "access_log"
     "rule_type" "average"
     "field" "upstream_response_time"
     "filters" []}

  4 {"datasource" "access_log"
     "rule_type" "ninety"
     "field" "upstream_response_time"
     "filters" []}
  })

(deftest parse-filter-test
  (let [rule-raw (get rules-test 1)
        datasource (get rule-raw "datasource")
        filter (parse-filter datasource (first (get rule-raw "filters")))]
    (is (= :equals (:operator filter)))
    (is (= :string (:field-type filter)))
    (is (true? (:negative filter))))
  (let [rule-raw (get rules-test 2)
        datasource (get rule-raw "datasource")
        filter-1 (parse-filter datasource (first (get rule-raw "filters")))
        filter-2 (parse-filter datasource (second (get rule-raw "filters")))]
    (is (= 302 (second (:content filter-1))))
    (is (instance? java.util.regex.Pattern (:content filter-2)))))

(deftest parse-rule-test
  (let [rule (parse-rule (str 2) (get rules-test 2))]
    (is (= :unique (:rule-type rule)))
    (is (= "remote_addr" (:field rule)))
    (is (= 2 (count (:filters rule))))))

(deftest parse-rules-test
  (let [children (into {} (for [[k v] rules-test]
                            [(str k) (util/json-encode v)]))
        rules (parse-rules children)]
    (is (seq (get rules "access_log")))))

(def logs-test
  {1 {"host" "shanghai.anjuke.com"
      "status" "200"
      "remote_addr" "1.2.3.4"}
   2 {"host" "www.anjuke.com"
      "status" "301"}
   3 {"upstream_response_time" "0.1"}})

(deftest rule-matches-test
  (let [get-result (fn [rule-id log-id]
                     (let [rule (parse-rule rule-id (get rules-test rule-id))]
                       (rule-matches rule (get logs-test log-id))))]
    (is (true? (get-result 1 1)))
    (is (false? (get-result 1 2)))
    (is (true? (get-result 2 1)))
    (is (false? (get-result 2 2)))))

(deftest alter-result-test
  (let [get-result (fn [rule-id log-id result]
                     (let [rule (parse-rule rule-id (get rules-test rule-id))]
                       (alter-result rule (get logs-test log-id) result)))]
    (is (= {1 2} (get-result 1 1 {1 1})))
    (is (= {2 #{"1.2.3.4"} 1 1} (get-result 2 1 {1 1})))
    (is (= {3 ["1.0" "0.1"]} (get-result 3 3 {3 ["1.0"]})))))

(deftest collect-result-inner-test
  (let [get-result (fn [rule-id values]
                     (let [rule (parse-rule rule-id (get rules-test rule-id))]
                       (collect-result-inner rule values)))]
    (is (= 1 (get-result 1 [1])))
    (is (= 2 (get-result 2 ["1.2.3.4" "1.2.3.4" "5.6.7.8"])))
    (is (= 1500 (get-result 3 ["1" "2"])))
    (is (= 9000 (get-result 4 (map str (range 1 11)))))))

(deftest peak-time-filter-test
  (binding [*date* (clj-time.core/local-date-time 2013 4 23)]
    (let [filter (first (peak-time-filter "access_log"))]
      (is (= (:field filter) "time_local"))
      (is (= "23/Apr/2013:10:" (:content filter))))
    (let [[filter1 filter2] (peak-time-filter "soj")]
      (is (= "stamp" (:field filter1)))
      (is (= :gte (:operator filter1)))
      (is (= 1366678800000 (:content filter1)))
      (is (= :lt (:operator filter2)))
      (is (= 1366682400000 (:content filter2))))))
