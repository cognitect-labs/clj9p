(defproject clj9p "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :dependencies [[org.clojure/clojure "1.8.0-beta1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha" :exclusions [[org.clojure/tools.analyzer.jvm]]]
                 [org.clojure/tools.analyzer.jvm "0.6.7"]
                 [io.netty/netty-all "4.0.30.Final"]]
  :global-vars {*warn-on-reflection* true
                *unchecked-math* :warn-on-boxed
                ;*compiler-options* {:disable-locals-clearing true}
                *assert* true}
  :pedantic? :abort
  :resource-paths ["resources" "config"])

