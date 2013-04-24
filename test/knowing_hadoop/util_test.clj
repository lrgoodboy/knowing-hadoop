(ns knowing-hadoop.util-test
  (:require [clj-time.core])
  (:use clojure.test
        knowing-hadoop.util))

(deftest get-config-test
  (let [conf-database (get-config :database)]
    (is (map? conf-database))
    (is (= (:knowing-db conf-database)
           (get-config :database :knowing-db)))))

(deftest split-line-test
  (is (= "bb" (second (split-line "aa\tbb"))))
  (let [delim (str (char 1))]
    (is (= "" (second (split-line (str "aa" delim delim "bb") delim))))))

(deftest current-minute-test
  (is (= 0 (clj-time.core/sec (current-minute)))))

(deftest millitime-test
  (is (pos? (millitime))))

(deftest yesterday-test
  (is (= (clj-time.core/day (clj-time.core/today))
         (clj-time.core/day (clj-time.core/plus (yesterday) (clj-time.core/days 1))))))

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
