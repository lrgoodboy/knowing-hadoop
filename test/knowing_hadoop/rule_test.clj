(ns knowing-hadoop.rule-test
  (:use clojure.test
        knowing-hadoop.rule))

(def demo-map {"host" "shanghai.anjuke.com"
               "status" "200"})

(deftest filter-log-test
  (println (filter-log "access_log" demo-map)))

(deftest get-datasources-test
  (let [datasources (get-datasources)]
    (is (= :numeric (get-in datasources
                            ["access_log" "upstream_response_time"])))
    (is (= :string (get-in datasources
                           ["soj" "guid"])))))

