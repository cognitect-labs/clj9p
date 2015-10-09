(defproject clj9p "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :global-vars {*warn-on-reflection* true
                *unchecked-math* :warn-on-boxed
                ;*compiler-options* {:disable-locals-clearing true}
                *assert* true}
  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha" :exclusions [[org.clojure/tools.analyzer.jvm]]]
                 [org.clojure/tools.analyzer.jvm "0.6.7"]
                 [io.netty/netty-all "4.0.30.Final"]])

