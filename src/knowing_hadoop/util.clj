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

(def ^:private zk-framework (ref nil))

(defn zk-connect []
  (if-not @zk-framework
    (dosync
      (ref-set zk-framework
               (CuratorFrameworkFactory/newClient
                 (clojure.string/join \, (get-config :zookeeper :hosts))
                 (RetryUntilElapsed. 3000 1000)))
      (.start @zk-framework)))
  @zk-framework)

(defn zk-ensure! [framework path]
  (let [client (.getZookeeperClient framework)
        ensurePath (.newNamespaceAwareEnsurePath framework path)]
    (.ensure ensurePath client)))

(defn zk-set! [framework path data]
  (zk-ensure! framework path)
  (.. framework setData (forPath path (.getBytes data))))

(defn zk-get [framework path]
 (String. (.. framework getData (forPath path))))

(defn zk-delete! [framework path]
  (.. framework delete (forPath path)))
