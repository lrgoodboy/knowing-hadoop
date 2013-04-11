(ns knowing-hadoop.util-test
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (is (seq (get-config :common :zookeeper-hosts))))
