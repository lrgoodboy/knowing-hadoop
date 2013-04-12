(ns knowing-hadoop.rule-test
  (:use clojure.test
        knowing-hadoop.rule))

(deftest get-datasources-test
  (let [datasources (get-datasources)]
    (is (= :numeric (get-in datasources
                            ["access_log" "upstream_response_time"])))
    (is (= :string (get-in datasources
                           ["soj" "guid"])))))

(deftest parse-filter-test
  (let [filter (parse-filter "access_log" ["host" "equals" true "www.anjuke.com"])]
    (is (= :equals (:operator filter)))
    (is (true? (:negative filter))))
  (let [filter (parse-filter "access_log" ["status" "nin" nil [301 302]])]
    (is (= 302 (second (:content filter))))))

(deftest parse-rule-test
  (let [rule (parse-rule 1 {"datasource" "access_log"
                            "rule_type" "unique"
                            "field" "remote_addr"
                            "filters" [["host" "equals" true "www.anjuke.com"]]})]
    (is (= :unique (:rule-type rule)))
    (is (= "remote_addr" (:field rule)))
    (is (seq (:filters rule)))))

(def demo-map {"host" "shanghai.anjuke.com"
               "status" "200"})

(deftest filter-log-test
  (println (filter-log "access_log" demo-map)))
