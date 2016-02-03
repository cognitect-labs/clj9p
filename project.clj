(defproject clj9p "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [io.netty/netty-all "4.0.34.Final"]
                 [com.barchart.udt/barchart-udt-bundle "2.3.0"]
                 [org.slf4j/slf4j-api "1.7.14"]]
  :global-vars {*warn-on-reflection* true
                *unchecked-math* :warn-on-boxed
                ;*compiler-options* {:disable-locals-clearing true}
                *assert* true}
  :pedantic? :abort
  :resource-paths ["resources" "config"])

