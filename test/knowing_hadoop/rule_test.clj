(ns knowing-hadoop.rule-test
  (:use clojure.test
        knowing-hadoop.rule))

(def demo-map {"host" "shanghai.anjuke.com"
               "status" "200"})

(deftest filter-log-test
  (println (filter-log "accesslog" demo-map)))
