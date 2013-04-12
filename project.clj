(defproject knowing-hadoop "0.1.0-SNAPSHOT"
  :description "Hadoop Job for Knowing."
  :url "http://knowing.corp.anjuke.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [clj-yaml "0.4.0"]
                 [com.netflix.curator/curator-framework "1.3.3"]
                 [org.clojure/data.json "0.2.2"]]
  :plugins [[lein2-eclipse "2.0.0"]])
