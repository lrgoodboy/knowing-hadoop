(ns knowing-hadoop.util
  (:require [clj-yaml.core :as yaml]))

(def ^:private config (ref {}))

(defn- read-config [section]
  {:post [map?]}
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
