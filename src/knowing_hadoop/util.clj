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

(def ^:private zk-client (ref nil))

(defn zk-connect []
  (if-not @zk-client
    (dosync
      (ref-set zk-client
               (CuratorFrameworkFactory/newClient
                 (clojure.string/join \, (get-config :zookeeper :hosts))
                 (RetryUntilElapsed. 3000 1000)))
      (.start @zk-client)))
  @zk-client)

(defn zk-ensure! [client path]
  (let [client-inner (.getZookeeperClient client)
        ensurePath (.newNamespaceAwareEnsurePath client path)]
    (.ensure ensurePath client-inner)))

(defn zk-set! [client path data]
  (zk-ensure! client path)
  (.. client setData (forPath path (.getBytes data))))

(defn zk-get [client path]
 (String. (.. client getData (forPath path))))

(defn zk-delete! [client path]
  (.. client delete (forPath path)))
