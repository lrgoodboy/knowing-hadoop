(ns knowing-hadoop.util
  (:require [clj-yaml.core :as yaml])
  (:import [com.netflix.curator.framework CuratorFrameworkFactory]
           [com.netflix.curator.retry RetryUntilElapsed]))

(def ^:private config (ref {}))

(defn- read-config [section]
  {:post [(map? %)]}
  (let [filename (str (name section) ".yaml")]
    (yaml/parse-string
      (slurp (clojure.java.io/resource filename)))))

(defn get-config

  ([section]
    (if-let [config-section (get @config section)]
      config-section
      (get (dosync
             (alter config assoc section (read-config section)))
           section)))

  ([section item]
    (get (get-config section) item)))

(defn zk-client []
  (CuratorFrameworkFactory/newClient
    (clojure.string/join \, (get-config :zookeeper :hosts))
    (RetryUntilElapsed. 3000 1000)))
