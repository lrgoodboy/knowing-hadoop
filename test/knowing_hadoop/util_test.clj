(ns knowing-hadoop.util-test
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (is (get-config :common)))

(deftest zk-connect-test
  (is (zk-connect)))

(def test-node (str "/knowing-hadoop-test-" (System/currentTimeMillis)))
(def test-content (str "test-" (System/currentTimeMillis)))

(deftest zk-get-set-test
  (let [client (zk-connect)
        stat-set (zk-set! client test-node test-content)
        data (zk-get client test-node)]
    (is (= test-content data))
    (zk-delete! client test-node)))
