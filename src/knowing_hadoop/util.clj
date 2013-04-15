(ns knowing-hadoop.util
  (:require [clj-yaml.core :as yaml]
            [clojure.data.json :as json])
  (:import [com.netflix.curator.framework CuratorFrameworkFactory]
           [com.netflix.curator.retry RetryUntilElapsed]))

(def ^:private config (atom {}))

(defn- read-config [section]
  {:post [(map? %)]}
  (let [filename (str (name section) ".yaml")]
    (yaml/parse-string
      (slurp (clojure.java.io/resource filename)))))

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

(defn zk-connect []
  (let [client (CuratorFrameworkFactory/newClient
                 (clojure.string/join "," (get-config :override :hosts))
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
