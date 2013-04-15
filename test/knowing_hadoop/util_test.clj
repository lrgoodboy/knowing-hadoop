(ns knowing-hadoop.util-test
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (is (get-config :common)))

(deftest zk-connect-test
  (is (.isStarted @zk-client)))

(def test-parent-node "/knowing-hadoop-test")
(def test-node-name (str "node-" (System/currentTimeMillis)))
(def test-node (str test-parent-node "/" test-node-name))
(def test-content (str "data-" (System/currentTimeMillis)))

(deftest zk-get-set-test
  (let [_ (zk-set! test-node test-content)
        data (zk-get test-node)]
    (is (= test-content data))
    (zk-delete! test-node)))

(deftest zk-get-children-test
  (let [_ (zk-set! test-node test-content)
        children (zk-get-children test-parent-node)]
    (is (= test-content (get children test-node-name)))
    (zk-delete! test-node)))
