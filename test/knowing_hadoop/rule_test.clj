(ns knowing-hadoop.rule-test
  (:require [knowing-hadoop.util :as util])
  (:use clojure.test
        knowing-hadoop.rule))

(deftest get-datasources-test
  (let [datasources (get-datasources)]
    (is (= :numeric (get-in datasources
                            ["access_log" "upstream_response_time"])))
    (is (= :string (get-in datasources
                           ["soj" "guid"])))))

(def rules-test
  {1 {"datasource" "access_log"
      "rule_type" "count"
      "field" nil
      "filters" [["host" "equals" true "www.anjuke.com"]]}

   2 {"datasource" "access_log"
      "rule_type" "unique"
      "field" "remote_addr"
      "filters" [["status" "nin" nil [301 302]]]}
   })

(deftest parse-filter-test
  (let [rule-raw (get rules-test 1)
        datasource (get rule-raw "datasource")
        filter (parse-filter datasource (first (get rule-raw "filters")))]
    (is (= :equals (:operator filter)))
    (is (true? (:negative filter))))
  (let [rule-raw (get rules-test 2)
        datasource (get rule-raw "datasource")
        filter (parse-filter datasource (first (get rule-raw "filters")))]
    (is (= 302 (second (:content filter))))))

(deftest parse-rule-test
  (let [rule (parse-rule 2 (get rules-test 2))]
    (is (= :unique (:rule-type rule)))
    (is (= "remote_addr" (:field rule)))
    (is (seq (:filters rule)))))

(deftest parse-rules-test
  (let [children (into {} (for [[k v] rules-test]
                            [k (util/json-encode v)]))
        rules (parse-rules children)]
    (is (seq (get rules "access_log")))))

(def demo-map {"host" "shanghai.anjuke.com"
               "status" "200"})

(deftest rule-matches-test
  (let [rule (parse-rule 1 (get rules-test 1))]
    (binding [*datasource* (:datasource rule)
              *log* demo-map]
      (let [result (rule-matches rule)]
        (is (= [1 nil] result))))))
