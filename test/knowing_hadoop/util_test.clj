(ns knowing-hadoop.util-test
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (is (get-config :common)))

(deftest zk-connect-test
  (is (zk-connect)))

(def test-parent-node "/knowing-hadoop-test")
(def test-node-name (str "node-" (System/currentTimeMillis)))
(def test-node (str test-parent-node "/" test-node-name))
(def test-content (str "data-" (System/currentTimeMillis)))

(deftest zk-get-set-test
  (let [client (zk-connect)
        _ (zk-set! client test-node test-content)
        data (zk-get client test-node)]
    (is (= test-content data))
    (zk-delete! client test-node)))

(deftest zk-get-children-test
  (let [client (zk-connect)
        _ (zk-set! client test-node test-content)
        children (zk-get-children client test-parent-node)]
    (is (= test-content (get children test-node-name)))
    (zk-delete! client test-node)))
