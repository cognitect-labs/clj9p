; Copyright 2019 Cognitect. All Rights Reserved.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;      http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS-IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(ns cognitect.clj9p.client
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.core.async :as async :refer [go-loop]]
            [cognitect.clj9p.9p :as n9p]
            [cognitect.clj9p.io :as io]
            [cognitect.clj9p.proto :as proto]
            [cognitect.net.netty.client :as nclient])
  (:import (io.netty.channel ChannelHandlerContext
                             Channel)
           (java.nio ByteOrder)
           (io.netty.buffer PooledByteBufAllocator)
           (io.netty.buffer ByteBuf)
           (io.netty.handler.codec LengthFieldBasedFrameDecoder)))


(extend-protocol n9p/Mountable

  clojure.lang.IFn
  (-mount [t from-server-chan]
    (let [in-chan (async/chan 10)]
      (go-loop []
               (when-let [fcall (async/<! in-chan)]
                 (if (async/>! from-server-chan (t fcall))
                   (recur)
                   ;; Otherwise we need to Clunk the root fid (which should be 1); Close the chan we created
                   (do (t (io/fcall {:type :tclunk :fid 1}))
                       (async/close! in-chan)))))
      in-chan))

  clojure.lang.APersistentVector
  (-mount [t from-server-chan]
    (let [[in-chan out-chan] t]
      (go-loop []
               (when-let [fcall-response (async/<! out-chan)]
                 (if (async/>! from-server-chan fcall-response)
                   (recur)
                   ;; Otherwise we need to Clunk the root fid (which should be 1)
                   (async/>! in-chan (io/fcall {:type :tclunk :fid 1})))))
      in-chan)))


(def default-initial-state
  {:version proto/version
   :root-fid 1
   :next-fid 2
   :open-fids {}
   :mounts {}
   :fs {}})

(defn client
  ([]
   (client {} (async/chan 10)))
  ([initial-state]
   (client initial-state (async/chan 10)))
  ([initial-state from-server-chan]
   (let [base-state (n9p/deep-merge default-initial-state initial-state)
         state-atom (atom base-state)]
     {:from-server from-server-chan
      :initial-state base-state
      :state state-atom})))

;; Public API
;; ----------

(defn find-mount-path [client full-path]
  (let [mounts (-> client :state deref :mounts)
        mount-paths (keys mounts)]
    (reduce (fn [match mount-path]
              (if (and (string/starts-with? full-path mount-path)
                       (> (count mount-path) (count match)))
                mount-path
                match))
            nil
            mount-paths)))

(defn call
  [client path-or-mount fcall-map]
  (let [mounts (-> client :state deref :mounts)
        best-mount (if (string? path-or-mount)
                     (some-> (find-mount-path client path-or-mount)
                             mounts
                             first)
                     path-or-mount)]
    (when (and best-mount (async/put! best-mount fcall-map))
      (:from-server client))))

(defn blocking-call
  [client path-or-mount fcall-map]
  (let [chan (call client path-or-mount fcall-map)]
    (when chan
      (async/<!! chan))))

(defn resolving-blocking-call
  [client path-or-mount fcall-map]
  (let [mounts (-> client :state deref :mounts)
        potential-mounts (if (string? path-or-mount)
                           (mounts (find-mount-path client path-or-mount))
                           [path-or-mount])]
    (loop [p-mounts potential-mounts
           result nil]
      (if (or (and result (not= (:type result) :rerror)) (empty? p-mounts))
        result
        (recur (rest p-mounts) (blocking-call client (first p-mounts) fcall-map))))))

(defn lazy-blocking-call
  [client path-or-mount fcall-map]
  (let [mounts (-> client :state deref :mounts)
        potential-mounts (if (string? path-or-mount)
                           (mounts (find-mount-path client path-or-mount))
                           [path-or-mount])]
    (map #(blocking-call client % fcall-map) potential-mounts)))

(defn attach [client mount-def path root-fid]
  (let [attach-result (blocking-call client mount-def (io/fcall {:type :tattach :fid root-fid}))]
    (when-not (get-in @(:state client) [:fs path :fid])
            (swap! (:state client)
                   assoc-in [:fs (subs path 1)] {:qid (:qid attach-result) :fid root-fid}))))

(defn mount [client spec]
  ;; Register the mounts within our client
  (let [out-chan (:from-server client)
        new-mounts (reduce (fn [mount-map [k v]]
                             (if (get-in @(:state client) [:mounts k])
                               mount-map ;; Don't re-mount things that are mounted
                               (assoc mount-map k (if (vector? v)
                                                    (mapv #(n9p/-mount % out-chan) v)
                                                    [(n9p/-mount v out-chan)]))))
                           {}
                           spec)
        root-fid (:root-fid @(:state client))]
    (swap! (:state client)
           update-in [:mounts] n9p/deep-merge new-mounts)
    ;; Version and attach each mount into the client's FS
    (doseq [[base-path mount-vec] new-mounts]
      (doseq [mount-def mount-vec]
        (let [vers-resp (blocking-call client mount-def (io/fcall {:type :tversion}))
              auth-resp (blocking-call client mount-def (io/fcall {:type :tauth
                                                                   :uname (System/getProperty "user.name")
                                                                   :aname ""
                                                                   :afid proto/NOFID}))]
          (attach client mount-def base-path root-fid))))
    client))

(defn clunk [client mount-def fid]
  (blocking-call client mount-def (io/fcall {:type :tclunk :fid fid})))

(defn unmount [client mount-path]
  "Unmount a single mount point, as specified by the string-path with which is
  was mounted.

  NOTE: If you unmount a channel-based (local) server, you will be closing the
  server's channels"
  (let [root-fid (:root-fid @(:state client))]
    (doseq [mount (flatten (get-in @(:state client) [:mounts mount-path]))]
      (clunk client mount root-fid)
      (async/close! mount))
    (swap! (:state client) update-in [:mounts] dissoc mount-path)
    (swap! (:state client) update-in [:fs] dissoc (subs mount-path 1))))

(defn unmount-all!
  "Unmount all file systems and remove the client's inbound channel.
  This safely shutsdown a client.
  A client may not be used after this call."
  [client]
  (doseq [[mount-path mounts] (:mounts @(:state client))]
    (unmount client mount-path))
  (async/close! (:from-server client))
  (reset! (:state client) (:initial-state client))
  (dissoc client :from-server))

(defn path-qid [client full-path]
  (get-in @(:state client) (concat [:fs] (remove empty? (string/split full-path #"/")) [:qid])))

(defn path-fid [client full-path]
  (get-in @(:state client) (concat [:fs] (remove empty? (string/split full-path #"/")) [:fid])))

;; Higher-level API - Blocking calls, always returns a value

(defn force-walk
  ([client full-path]
   (force-walk client full-path (:next-fid @(:state client)) (find-mount-path client full-path)))
  ([client full-path newfid]
   (force-walk client full-path newfid (find-mount-path client full-path)))
  ([client full-path newfid mount-path]
   (if mount-path
     (let [server-path-parts (vec (remove empty? (string/split (str "/" (subs full-path (count mount-path))) #"/")))
           state @(:state client)
           walk-results (into [] (distinct
                                   (lazy-blocking-call client
                                                       mount-path
                                                       (io/fcall {:type :twalk :fid (:root-fid state) :newfid newfid :wname server-path-parts}))))
           _ (when (every? #(= (:type %) :rerror) walk-results)
               (throw (ex-info "Failed client walk" {:client client
                                                     :path full-path
                                                     :error-response walk-results})))
           partial-fs (reduce (fn [fs-map [path-piece qid]]
                                (let [new-parts (conj (:parts fs-map) path-piece)]
                                  {:parts new-parts
                                   :fs (assoc-in (:fs fs-map) new-parts {:qid qid})}))
                              {:parts [(subs mount-path 1)]
                               :fs {}}
                              (distinct (mapcat #(map vector server-path-parts (:wqid %)) walk-results)))
           updated-fs (assoc-in (:fs partial-fs) (conj (:parts partial-fs) :fid) newfid)
           next-state (-> state
                          (update :next-fid inc) ;; just in case newfid was :next-fid
                          (update :fs n9p/deep-merge updated-fs))]
       (reset! (:state client) next-state)
       client)
     client)))

(defn walk
  "Given a client map and a string of a full-path,
  perform a walk and return a client with an updated `:fs` state - containing
  the additional information obtained from the walk."
  [client full-path]
  (if-let [mount-path (and (not (path-fid client full-path))
                           (find-mount-path client full-path))]
    (force-walk client full-path)
    client))

(defn closefid [client mount-path fid]
  (let [;; TODO: This fid only exists on a certain server, we need to track/know that
        full-path (get-in @(:state client) [:open-fids fid :path])
        resp (resolving-blocking-call client mount-path (io/fcall {:type :tclunk :fid fid}))]
    ;; When there are no errors, remove any associated open fids
    (if (= (:type resp) :rerror)
      (throw (ex-info "Failed client clunk for closefid" {:client client
                                                          :mount-path mount-path
                                                          :full-path full-path
                                                          :error-response resp}))
      (do (swap! (:state client) update-in [:open-fids] dissoc fid)
          (swap! (:state client) update-in (conj (rest (string/split full-path #"/")) :fs) dissoc :fid)
          ;; Re-establish the FID.  If the FID was a Root, reattach it.
          (if (= fid (:root-fid @(:state client)))
            (attach client mount-path full-path fid)
            (force-walk client full-path fid mount-path))))
    client))

(defn close [client full-path]
  (when-let [fid (first (keep (fn [[fid open-map]]
                                (when (= full-path (:path open-map))
                                  fid))
                              (:open-fids @(:state client))))]
    (let [mount-path (find-mount-path client full-path)
          ;; TODO: This fid only exists on a certain server, we need to track/know that
          resp (resolving-blocking-call client mount-path (io/fcall {:type :tclunk :fid fid}))]
      ;; When there are no errors, remove any associated open fids
      (if (= (:type resp) :rerror)
        (throw (ex-info "Failed client clunk for close" {:client client
                                                         :path full-path
                                                         :error-response resp}))
        (do (swap! (:state client) update-in [:open-fids] dissoc fid)
            (swap! (:state client) update-in (conj (rest (string/split full-path #"/")) :fs) dissoc :fid)))))
  client)

(defn base-open [client mount-path fid mode]
  ;; On the surface, this should be `resolving-blocking-call` - opening the best match possible,
  ;; But in the case where you're opening a dir that many fs's contribute to, you need to
  ;; open all of them.
  (distinct (lazy-blocking-call client mount-path (io/fcall {:type :topen :fid fid :mode mode}))))

(defn open
  ([client full-path]
   (open client full-path proto/OREAD))
  ([client full-path mode]
   (if-let [already-opened (get-in @(:state client) [:open-fids (path-fid client full-path)])]
     (cond
       (= mode (:mode already-opened)) client
       :else (do (close client full-path)
                 (open client full-path mode)))
     (let [walked (walk client full-path)
           mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
           fid (path-fid client full-path)
           responses (base-open client mount-path fid mode)]
       (if (every? #(= (:type %) :rerror) responses)
         (throw (ex-info "Failed client open" {:client client
                                               :path full-path
                                               :mode mode
                                               :error-responses responses}))
         (doseq [resp (remove #(= (:type %) :rerror) responses)]
           (swap! (:state client) assoc-in [:open-fids fid] (assoc resp
                                                                   :mode mode
                                                                   :path full-path))
           client))))))

(defn full-read
  ([client full-path]
   (full-read client full-path 0 0))
  ([client full-path byte-count]
   (full-read client full-path byte-count 0))
  ([client full-path byte-count offset]
   (let [walked (walk client full-path)
         mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
         fid (path-fid client full-path)
         responses (into [] (distinct (lazy-blocking-call client mount-path (io/fcall {:type :tread :fid fid :offset offset :count byte-count}))))]
     (if (every? #(= (:type %) :rerror) responses)
       (throw (ex-info "Failed client full-read" {:client client
                                                  :path full-path
                                                  :byte-count byte-count
                                                  :offset offset
                                                  :error-responses responses}))
       (remove nil? (map :data responses))))))

(defn read
  ([client full-path]
   (read client full-path 0 0))
  ([client full-path byte-count]
   (read client full-path byte-count 0))
  ([client full-path byte-count offset]
   ;; TODO: The client technically needs to open the fid before read to be spec compliant
   (let [walked (walk client full-path)
         mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
         fid (path-fid client full-path)
         resp (resolving-blocking-call client mount-path (io/fcall {:type :tread :fid fid :offset offset :count byte-count}))]
     (if (= (:type resp) :rerror)
       (throw (ex-info "Failed client read" {:client client
                                             :path full-path
                                             :byte-count byte-count
                                             :offset offset
                                             :error-response resp}))
       (:data resp)))))

(defn read-str
  ([client full-path]
   (read-str client full-path io/default-message-size-bytes 0))
  ([client full-path byte-count]
   (read-str client full-path byte-count 0))
  ([client full-path byte-count offset]
   (let [data (read client full-path byte-count offset)]
     (if (string? data) data (String. ^bytes data "UTF-8")))))

(defn write
  ([client full-path data]
   (write client full-path data 0))
  ([client full-path data offset]
   (let [walked (walk client full-path)
         mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
         fid (path-fid client full-path)
         resp (resolving-blocking-call client mount-path (io/fcall {:type :twrite :fid fid :offset offset :data data}))]
     (if (= (:type resp) :rerror)
       (throw (ex-info "Failed client read" {:client client
                                             :path full-path
                                             :data data
                                             :offset offset
                                             :error-response resp}))
       (:count resp)))))

(defn stat [client full-path]
  (let [walked (walk client full-path)
        mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
        fid (path-fid client full-path)]
     (resolving-blocking-call client mount-path (io/fcall {:type :tstat :fid fid}))))

(defn edn-read-fn [x]
  (edn/read-string (if (string? x)
                     x
                     (and x (String. ^bytes x "UTF-8")))))

(defn binstat-read-fn [x]
  (when x
    (let [buffer (io/default-buffer x)]
      (io/read-stats buffer false false))))

(defn ls
  ([client full-path]
   (ls client full-path binstat-read-fn))
  ([client full-path dir-read-fn]
   (let [walked (walk client full-path)
         mount-path (find-mount-path client full-path) ;; We know it's good because the walk passed
         fid (path-fid client full-path)
         old-open (get-in @(:state client) [:open-fids fid])
         open-map (or old-open
                      (do (open client full-path)
                          (get-in @(:state client) [:open-fids fid])))
        qid (path-qid client full-path)]
    (if (= proto/QTDIR (:type qid))
      (let [read-data (full-read client full-path io/default-message-size-bytes 0)
            _ (closefid client mount-path fid)
            _ (when old-open
                (open client full-path (:mode old-open)))]
        (try (into []
                   (vals
                     (reduce (fn [result e]
                              (if (result (:name e)) ;; If we already have a file by that name...
                                result ;; Ignore it and move on...
                                (assoc result (:name e) e))) ;; Otherwise, add the new file to our results
                            {}
                            (flatten (map dir-read-fn read-data)))))
             (catch Throwable t
               (println "Failure to read the ls results")
               (clojure.stacktrace/print-stack-trace t)
               read-data)))
      (and (path-qid client full-path)
           (last (string/split full-path #"/")))))))

(defn touch
  ([client full-path]
   (touch client full-path 0644 1))
  ([client full-path permission]
   (touch client full-path permission 1))
  ([client full-path permission mode]
   (let [file-name (subs full-path (string/last-index-of full-path "/"))
         base-path (subs full-path 0 (string/last-index-of full-path "/"))
         walked (walk client base-path)
         mount-path (find-mount-path client base-path) ;; We know it's good because the walk passed
         fid (path-fid client base-path)
         old-open (get-in @(:state client) [:open-fids fid])
         open-map (or old-open
                      (do (open client base-path proto/OWRITE)
                          (get-in @(:state client) [:open-fids fid])))
         resp (resolving-blocking-call client mount-path (io/fcall {:type :tcreate :fid fid :name file-name :mode mode :perm permission}))
         _ (closefid client mount-path fid)
         _ (when old-open
                (open client base-path (:mode old-open)))]
     (if (= (:type resp) :rerror)
       (throw (ex-info "Failed client create" {:client client
                                               :path full-path
                                               :error-response resp}))
       (walk client full-path)))))

(defn lsofids [client]
  (:open-fids @(:state client)))

(defn lsof [client]
  (->> (lsofids client)
      vals
      (map #(select-keys % [:path :mode]))))

(defn fsiounit [client full-path]
  (when-let [opened-fd (get-in @(:state client) [:open-fids (path-fid client full-path)])]
    (:iounit opened-fd)))


(defn file-type [client full-path]
  (let [walked (walk client full-path)
        qid (path-qid client full-path)]
    (:type qid)))

(defn directory? [client full-path]
  (io/directory? (file-type client full-path)))

(defn file? [client full-path]
  (io/file? (file-type client full-path)))

(defn mode
  ([stat-map]
   (io/mode-str (:mode stat-map)))
  ([client full-path]
   (-> (stat client full-path)
       :stat
       first
       mode)))

(defn remote-connect [channel-class client-options]
  (let [to-server (async/chan 10)
        from-server (async/chan 10)
        framer (LengthFieldBasedFrameDecoder. ByteOrder/LITTLE_ENDIAN
                                              (:max-msg-size client-options io/default-message-size-bytes)
                                              0 4 -4 0
                                              true)
        clnt (nclient/client (merge client-options {:join? false})
                             channel-class
                             [framer
                              {:channel-read (fn [ctx msg]
                                               (let [buffer (cast ByteBuf msg)
                                                     fcall (io/decode-fcall! buffer {})]
                                                 ;; Ensure backpressure bubbles up
                                                 (when-not (async/>!! from-server
                                                                      (assoc fcall
                                                                             ::buffer buffer
                                                                             ::remote ctx))
                                                   (.. ctx (channel) (close))
                                                   (.. ctx (channel) (parent) (close)))))}])
        connected-client (nclient/start clnt)]
    ;; Setup the to-server go-loop
    (go-loop []
     (if-let [output-fcall (async/<! to-server)]
       (do
         (.writeAndFlush @(:remote-context connected-client)
                 (io/encode-fcall! output-fcall (.directBuffer PooledByteBufAllocator/DEFAULT)))
         (recur))
       (do (async/close! from-server)
           (nclient/stop connected-client))))
    [to-server from-server]))

(defn tcp-connect [client-options]
  (remote-connect nclient/tcp-channel-class client-options))

(defn sctp-connect [client-options]
  (remote-connect nclient/sctp-channel-class client-options))

(defn udt-connect [client-options]
  (remote-connect nclient/udt-channel-factory client-options))

