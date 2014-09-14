(defproject acyclic/awstools "0.1.0-SNAPSHOT"
  :author "Peter Fraenkel <http://podsnap.com>"
  :description "Simple tools for controlling EC2 using core.async"
  :url "http://github.com/pnf/awstools"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [clj-time "0.7.0"]
                 [amazonica "0.2.25"]
                 [org.clojure/data.json "0.2.5"]
                 [acyclic/utils "0.1.0-SNAPSHOT"]
                 [clj-ssh "0.5.10"]
                 [org.clojure/core.async "0.1.338.0-5c5012-alpha"]
                 ;[org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [com.taoensso/timbre "3.3.1"] 
                 [com.taoensso/carmine "2.7.0"]])



