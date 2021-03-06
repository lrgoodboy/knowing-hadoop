(ns knowing-hadoop.util
  (:require [clojure.data.json :as json]
            [clj-time.core]
            [clj-time.format]
            [clj-time.local]
            [clj-time.coerce]
            [clojure.tools.logging :as logging])
  (:import [com.netflix.curator.framework CuratorFrameworkFactory]
           [com.netflix.curator.retry RetryUntilElapsed]))

(def ^:private config (atom {}))

(defn- read-config [section]
  (let [read (fn [res-path]
               {:post [(map? %)]}
               (if-let [res (clojure.java.io/resource res-path)]
                 (read-string (slurp res))
                 {}))
        default-name (str (name section) ".clj")
        default (read default-name)
        override (read (str "override/" default-name))]
    (merge default override)))

(defn get-config

  ([section]
    (if-let [config-section (get @config section)]
      config-section
      (let [config-section (read-config section)]
        (swap! config assoc section config-section)
        config-section)))

  ([section item]
    (get (get-config section) item)))

(defn json-decode [s]
  (try
    (json/read-str s)
    (catch Exception e)))

(defn json-encode [o]
  (json/write-str o))

(defn in-array [value array]
  ((complement nil?) (some (partial = value) array)))

(defn current-minute []
  (let [now (clj-time.local/local-now)]
    (clj-time.core/local-date-time (clj-time.core/year now)
                                   (clj-time.core/month now)
                                   (clj-time.core/day now)
                                   (clj-time.core/hour now)
                                   (clj-time.core/minute now))))

(defn millitime []
  (let [now (clj-time.local/local-now)] ; (local-now) returns a DateTime object with default timezone.
    (clj-time.coerce/to-long now)))

(defn yesterday []
  (clj-time.core/minus (clj-time.core/today) (clj-time.core/days 1)))

(defn parse-double [x]
  (try
    (Double/parseDouble x)
    (catch Exception e)))

(defn parse-ymd [date-str]
  (clj-time.format/parse-local-date (:date clj-time.format/formatters) date-str))

(defn unparse-ymd [date]
  (clj-time.format/unparse-local (:date clj-time.format/formatters) date))

(defmacro time-it [label expr & [sampling]]
  (let [sampling (if (nil? sampling) 0 sampling)]
    `(if (zero? (Math/round (rand ~sampling)))
       (let [start# (System/nanoTime)
             ret# ~expr
             elapsed# (/ (- (System/nanoTime) start#) 1000000.0)]
         (logging/info (format "Elapsed time - %s: %.3f ms" ~label elapsed#))
         ret#)
       ~expr)))

(defn zk-connect []
  (let [client (CuratorFrameworkFactory/newClient
                 (clojure.string/join "," (get-config :zookeeper :hosts))
                 (RetryUntilElapsed. 3000 1000))]
    (.start client)
    client))

(def zk-client (delay (zk-connect)))

(defn zk-ensure! [path]
  (let [client-inner (.getZookeeperClient @zk-client)
        ensurePath (.newNamespaceAwareEnsurePath @zk-client path)]
    (.ensure ensurePath client-inner)))

(defn zk-set! [path data]
  (zk-ensure! path)
  (.. @zk-client setData (forPath path (.getBytes data))))

(defn zk-get [path]
  (String. (.. @zk-client getData (forPath path))))

(defn zk-delete! [path]
  (.. @zk-client delete (forPath path)))

(defn join-path [path file]
  (if (.endsWith path "/")
    (str path file)
    (str path "/" file)))

(defn zk-get-children [path]
  (into {} (for [child (.. @zk-client getChildren (forPath path))]
             [child (zk-get (join-path path child))])))
