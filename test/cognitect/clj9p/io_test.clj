(ns cognitect.clj9p.io-test
  (:require [clojure.test :refer :all]
            [cognitect.clj9p.io :as io]
            [cognitect.clj9p.proto :as proto]))

(defn round-trip [fcall-map buffer]
  (io/decode-fcall!
    (io/encode-fcall! fcall-map buffer)
    {}))

(defn round-trip?
  ([fcall-map buffer]
   (round-trip? fcall-map buffer []))
  ([fcall-map buffer dissoc-keys]
   (let [expected (io/fcall fcall-map)
         actual (round-trip expected buffer)]
     (= (apply dissoc actual :original-size dissoc-keys) expected))))

(deftest round-trip-encoding
  (let [buffer (io/default-buffer)]
    (testing "Version messages round-trip"
      (is (round-trip? {:type :tversion :msize 10} buffer))
      (is (round-trip? {:type :rversion :msize 10} buffer)))
    (testing "Auth messages round-trip"
      (is (round-trip? {:type :tauth
                        :afid 10
                        :uname "ohpauleez"
                        :aname "testattach1"} buffer))
      (is (round-trip? {:type :rauth
                        :aqid {:type proto/QTDIR :path 42 :version 1}} buffer)))
    (testing "Error messages round trip"
      (is (round-trip? {:type :rerror
                        :ename "Bogus File Read"} buffer)))
    (testing "Walk messages round trip"
      (is (round-trip? {:type :twalk
                        :fid 1
                        :newfid 2
                        :wname ["something"]} buffer))
      #_(is (round-trip? {:type :rwalk})))
    (testing "Stat messages round trip"
      (let [fcall-map {:type :rstat
                        :stat [{:name "databases"
                                :qid {:type proto/QTDIR :version 0 :path 1502332285}
                                :mode (+ proto/DMDIR 0755)
                                :atime (io/now) :mtime (io/now)
                                :uid "dbuser" :gid "dbuser" :muid "dbuser"
                                :type 0 :dev 0 :statsz 0 :length 0}]}
            expected (io/fcall fcall-map)
         actual (round-trip expected buffer)]
     (is (= (mapv #(dissoc % :atime :mtime :statsz) (:stat actual))
            (mapv #(dissoc % :atime :mtime :statsz) (:stat expected))))))))

