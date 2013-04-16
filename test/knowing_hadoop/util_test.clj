(ns knowing-hadoop.util-test
  (:require [clj-time.core])
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (is (get-config :common)))

(deftest parse-line-test
  (is (= "b" (second (parse-line "a b c"))))
  (let [delim (str (char 1))]
    (is (= "" (second (parse-line (str "a" delim delim "b") delim))))))

(deftest current-minute-test
  (is (= 0 (clj-time.core/sec (current-minute)))))

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
