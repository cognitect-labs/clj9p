(ns cognitect.clj9p.server
  (:require [clojure.core.async :as async]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as string]
            [cognitect.clj9p.9p :as n9p]
            [cognitect.clj9p.io :as io]
            [cognitect.clj9p.proto :as proto]
            [cognitect.net.netty.server :as netty])
  (:import (io.netty.buffer ByteBuf)
           (io.netty.buffer PooledByteBufAllocator)
           (io.netty.channel ChannelHandlerContext
                             Channel
                             ChannelPipeline)
           (io.netty.handler.codec LengthFieldBasedFrameDecoder)
           (java.nio ByteOrder)))

(extend-protocol n9p/Remote
  nil
  (get-remote-id [t] t)

  ChannelHandlerContext
  (get-remote-id [t]
    (n9p/get-remote-id (.channel t)))

  Channel
  (get-remote-id [t]
    (.remoteAddress t)))

;; All handlers take and return a `context`.
;; The context is a map that contains at least two keys -
;; `:server-state` and `:input-fcall`.  Adding an `:output-fcall` to
;; a context will encode the :output-fcall and place it on the file descriptor.
;; Server state changes are also reflected via the context.

;; NOTE: YOU SHOULD ALWAYS BUILD OUTPUT FCALLS FROM INPUT FCALLS
;;       -- servers, like Netty, smuggle data within fcalls --

;; Auxiliary functions
;; --------------------
(defn make-resp
  "An auxiliary function when your context modifications only
  involve adding an output-fcall"
  [ctx resp-map]
  (assoc ctx :output-fcall (into (:input-fcall ctx) resp-map)))

(defn rerror
  [ctx ename]
  (println "Error happened with fcall:" (:input-fcall ctx))
  (make-resp ctx {:type :rerror :ename ename}))

(defn unknown-fid
  [ctx fid]
  (println "Fcall for unkown fid" (:input-fcall ctx))
  (rerror ctx (str "Unknown fid: " fid)))

(defn directory? [qid]
  (pos? (bit-and (:type qid) proto/QTDIR)))

(defn- assign-fid-thunk [remote fid uname qid]
  (fn [state]
    (assoc-in state [:client-fids remote fid] {:uname uname
                                               :qid qid
                                               :open-mode -1
                                               ;; TODO: Handle auth cases
                                               :auth? false})))

(defn- unassign-fid-thunk [remote fid]
  (fn [state]
   (update-in state [:client-fids remote] dissoc fid)))


;; Response functions/handlers
;; ---------------------------

(defn tversion
  [context]
  (let [request-version (get-in context [:input-fcall :version])
        version (if (string/starts-with? request-version "9P2000")
                  request-version
                  "unknown")]
    (make-resp context {:type :rversion
                        :version version})))

(defn tauth
  [context]
  ;; TODO Add Auth support
  (rerror context "Auth not required"))


(defn tattach
  [context]
  (let [input-fcall       (:input-fcall context)
        remote            (n9p/get-remote-id (::remote input-fcall))
        client-fids       (get-in context [:server-state :client-fids remote] {})
        {:keys [fs root]} (:server-state context)
        attach-fn         (get-in context [:server-state :ops :attach])
        fid               (:fid input-fcall)
        root-qid          (:qid root)]
    ;; TODO: Add afid handling
    (cond
      (client-fids fid) (rerror context (str "Duplicate fid for client: " fid))
      attach-fn         (attach-fn context {})
      :else             (-> context
                            (assoc :server-state-updater (assign-fid-thunk remote fid (:uname input-fcall) root-qid))
                            (make-resp {:type :rattach
                                        :qid  root-qid})))))

(defn tflush
  [context]
  (if-let [flush-fn (get-in context [:server-state :ops :flush])]
    (flush-fn context {})
    (make-resp context {:type :rflush})))

(defn twalk
  [context]
  (let [input-fcall  (:input-fcall context)
        remote       (n9p/get-remote-id (::remote input-fcall))
        input-fid    (:fid input-fcall)
        input-newfid (:newfid input-fcall)
        fid          (get-in context [:server-state :client-fids remote input-fid])
        newfid       (get-in context [:server-state :client-fids remote input-newfid])
        qid          (:qid fid)
        root-qid     (get-in context [:server-state :root :qid])
        file-walk    (get-in context [:server-state :fs qid :walk]
                             (get-in context [:server-state :ops :walk]))]
    (cond
      (nil? fid)
      (unknown-fid context input-fid)

      (not= (:open-mode fid) -1)
      (rerror context "Cannot clone an open fid")

      (and (pos? (count (:wname input-fcall)))
           (not (directory? qid)))
      (rerror context "Cannot walk in non-directory")

      (and (not= input-fid input-newfid) newfid)
      (rerror context (str "newfid is a duplicate fid for client: " input-newfid))

      (zero? (count (:wname input-fcall)))
      (-> context
          (assoc :server-state-updater (assign-fid-thunk remote input-newfid (:uname input-fcall "") qid))
          (make-resp {:type :rwalk
                      :wqid []}))

      file-walk
      (file-walk context qid)

      :else
      (rerror context "No walk function"))))

(defn topen
  [context]
  (let [input-fcall (:input-fcall context)
        remote (n9p/get-remote-id (::remote input-fcall))
        input-fid (:fid input-fcall)
        fid (get-in context [:server-state :client-fids remote input-fid])
        qid (:qid fid)
        root-qid (get-in context [:server-state :root :qid])
        file-open (get-in context [:server-state :fs qid :open]
                          (get-in context [:server-state :ops :open]))
        open-mode-thunk (fn [open-mode]
                          (fn [state]
                            (assoc-in state [:client-fids remote input-fid :open-mode] open-mode)))]

    (cond
      (nil? fid)
      (unknown-fid context input-fid)

      (not= (:open-mode fid) -1)
      (rerror context "Botched 9p call: FID is already in an open state")

      ;; TODO: Also force access permissions
      (and (directory? qid)
           (not= (bit-and (:mode input-fcall)
                          (bit-not proto/ORCLOSE)) proto/OREAD))
      (rerror context "FD is a directory, mode not allowed")

      file-open
      (file-open context qid) ;; It is expected that server implementations correctly set open perms

      (and (nil? (get-in context [:server-state :fs qid]))
           (not= qid root-qid))
      (unknown-fid context input-fid)

      :else
      (-> context
          (assoc :server-state-updater (open-mode-thunk (:mode input-fcall))) ;; If no implementation, we just open as told regardless
          (make-resp {:type :ropen
                      :qid qid
                      :iounit (- ^long (:iounit input-fcall) ^long proto/IOHDRSZ)})))))

(defn tcreate
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        file-create (get-in context [:server-state :fs qid :create]
                            (get-in context [:server-state :ops :create]))]
    (cond
      (nil? fid)                     (unknown-fid context input-fid)
      (not= (:open-mode fid) -1)     (rerror context "Botched 9P call: Cannot create in a non-open'd descriptor")

      (not (directory? qid))         (rerror context "Cannot create in a non-directory")
      file-create                    (file-create context qid)
      :else                          (rerror context "No create function"))))

(defn tread
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        read-count  (:count input-fcall)
        read-count  (if (> ^long read-count (- ^long (:msize input-fcall) ^long proto/IOHDRSZ))
                      (- ^long (:msize input-fcall) ^long proto/IOHDRSZ)
                      read-count)
        file-read   (get-in context [:server-state :fs qid :read]
                            (get-in context [:server-state :ops :read]))]
    (cond
      (nil? fid)              (unknown-fid context input-fid)
      (neg? ^long read-count) (rerror context "Botched 9P call - `count` was negative on read")
      ;; TODO: Add auth handling
      ;; TODO: Enforce permissions and access
      file-read               (file-read context qid)
      :else                   (rerror context "No read function"))))

(defn twrite
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        write-count (:count input-fcall (count (:data input-fcall)))
        write-count (if (> ^long write-count ^long (- ^long (:msize input-fcall) ^long proto/IOHDRSZ))
                      (- ^long (:msize input-fcall) ^long proto/IOHDRSZ)
                      write-count)
        file-write  (get-in context [:server-state :fs qid :write]
                            (get-in context [:server-state :ops :write]))]
    (cond
      (nil? fid)               (unknown-fid context input-fid)
      (neg? ^long write-count) (rerror context "Botched 9P call - `count` was negative on write")
      ;; TODO: Add auth handling
      ;; TODO: Enforce permissions and access
      file-write               (file-write context qid)
      :else                    (rerror context "No write function"))))

(defn tclunk
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        file-clunk  (get-in context [:server-state :fs qid :clunk]
                            (get-in context [:server-state :ops :clunk]))]
    (cond
      (nil? fid) (unknown-fid context input-fid)
      file-clunk (file-clunk context qid)
      ;; There is no rclunk support (not needed); Drop the fid
      :else      (-> context
                     (assoc :server-state-updater (unassign-fid-thunk remote input-fid))
                     (make-resp {:type :rclunk})))))

(defn tremove
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        file-remove (get-in context [:server-state :fs qid :remove]
                            (get-in context [:server-state :ops :remove]))]
    (cond
      (nil? fid)  (unknown-fid context input-fid)
      file-remove (file-remove context qid)
      ;; There is no rremove support (not needed)
      :else       (assoc context :server-state-updater (unassign-fid-thunk remote input-fid)))))

(defn tstat
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        file-stat   (get-in context [:server-state :fs qid :stat]
                            (get-in context [:server-state :ops :stat]))
        stat-info   (get-in context [:server-state :fs qid :stat-info])]
    (cond
      (nil? fid) (unknown-fid context input-fid)
      stat-info  (make-resp context
                            {:type :rstat
                             :stat [(merge {:qid qid}
                                           stat-info)]})
      file-stat  (file-stat context qid)
      :else      (rerror context "No stat function"))))

(defn twstat
  [context]
  (let [input-fcall (:input-fcall context)
        remote      (n9p/get-remote-id (::remote input-fcall))
        input-fid   (:fid input-fcall)
        fid         (get-in context [:server-state :client-fids remote input-fid])
        qid         (:qid fid)
        file-wstat  (get-in context [:server-state :fs qid :wstat]
                            (get-in context [:server-state :ops :wstat]))]
    (cond
      (nil? fid) (unknown-fid context input-fid)
      file-wstat (file-wstat context qid)
      :else      (rerror context "No wstat function"))))

(defn reporting-ex-handler [^Throwable t ctx]
  (rerror ctx (str "There was a server error when handling the " (get-in ctx [:input-fcall :type])
                   "msg.\nReason: " t)))

(def default-handlers
  {:tversion tversion
   :tauth tauth
   :tattach tattach
   :tflush tflush
   :twalk twalk
   :topen topen
   :tcreate tcreate
   :tread tread
   :twrite twrite
   :tclunk tclunk
   :tremove tremove
   :tstat tstat
   :twstat twstat
   ;; The `ex-handler` is used to rescue bad requests and produce valid r-messages that report the error
   :ex-handler reporting-ex-handler})

(defn server-handlers [override-map]
  (merge default-handlers
         override-map))

(def default-initial-state {:client-fids {} ;; Map each client's fids to qids on the server
                            ;; The root stat of your filesystem
                            :root {:type 0
                                   :dev 0
                                   :mode (+ ^long proto/DMDIR 0755)
                                   :length 0
                                   :atime (io/now)
                                   :mtime (io/now)
                                   :name "/"
                                   :qid (with-meta
                                          {:type proto/QTDIR
                                           :version 0
                                           :path (io/path-hash "/")}
                                          {::string-path "/"})}
                            :atime (io/now)
                            ;; This is your "file system"
                            :fs {}
                            ;; These are fallback ops if a file in the fs doesn't implement them
                            :ops {}})

;; Common override handler utilities
;; ----------------------------------

(defn fake-stat [ctx qid]
  (if-let [stat-info (get-in ctx [:server-state :fs qid :stat-info])]
    stat-info
    {:type 0
     :dev 0
     :statsz 0
     :qid qid
     :mode (if (= (:type qid) proto/QTDIR)
             (+ ^long proto/DMDIR 0755)
             0644)
     :atime (:atime default-initial-state)
     :mtime (io/now)
     :length 0 ;; 0 Length is used for directories and devices/services
     :name (or (get-in ctx [:server-state qid :name]
                       (some-> (meta qid)
                               ::string-path
                               (string/split #"/")
                               last))
               "")
     :uid "user"
     :gid "user"
     :muid "user"}))

(defn stat-faker
  "A helper for faking stat calls"
  [ctx qid]
  ;; Just fake the stat for all "files"
  (assoc ctx
         :output-fcall (assoc (:input-fcall ctx)
                              :type :rstat
                              :stat [(fake-stat ctx qid)])))

(defn path->qid [fs path]
  (let [clean-path (string/replace path #"/+" "/")]
    (first (filter #(= (::string-path (meta %))
                       clean-path)
                   (keys fs)))))

(defn butlast-path [path]
  (subs path 0 (string/last-index-of path "/")))

(defn qid-path [qid]
  (::string-path (meta qid)))

(defn qid-parent-path [fs qid]
  (butlast-path (qid-path qid)))

(defn qid-children-qids [fs qid]
  (let [path (qid-path qid)
        path-pattern (re-pattern (str path "/[^/]+$"))]
    (if (= path "/")
      (filter #(re-find #"^/[^/]+$" (qid-path %)) (keys fs))
      (filter #(re-find path-pattern (qid-path %)) (keys fs)))))

(defn path-walker
  [ctx qid]
  ;; this should just be a loop recur, since we could have ".." at any level
  (let [input-fcall            (:input-fcall ctx)
        remote                 (n9p/get-remote-id (::remote input-fcall))
        {:keys [wname newfid]} input-fcall
        input-fid              (:fid input-fcall)
        fid                    (get-in ctx [:server-state :client-fids remote input-fid])
        root-qid               (get-in ctx [:server-state :root :qid])
        fs                     (get-in ctx [:server-state :fs])
        current-path           (::string-path (meta qid) "/")
        wqid                   (loop [remaining-path-parts wname
                                      path current-path
                                      wqid []]
                                 (cond
                                   (empty? remaining-path-parts) wqid
                                   (= (first remaining-path-parts) "..") (if (= path "/")
                                                                           wqid
                                                                           (recur (rest remaining-path-parts)
                                                                                  (butlast-path path)
                                                                                  (conj wqid (path->qid fs (butlast-path path)))))
                                   :else (recur (rest remaining-path-parts)
                                                (str path "/" (first remaining-path-parts))
                                                (conj wqid (path->qid fs (str path "/" (first remaining-path-parts)))))))]
    (if (some nil? wqid)
      (rerror ctx "Path/File not found; Path may be incomplete or inconsistent.")
      (-> ctx
          (assoc :server-state-updater (assign-fid-thunk remote newfid (:uname fid) (or (last wqid) root-qid)))
          (make-resp {:type :rwalk
                      :wqid wqid})))))

(defn clj-dirreader
  [ctx qid]
  (if-let [child-qids (and (= (:type qid) proto/QTDIR)
                           (qid-children-qids (get-in ctx [:server-state :fs]) qid))]
    (let [stat-str (pr-str (mapv #(fake-stat ctx %) child-qids))]
      (make-resp ctx {:type :rread
                      :data stat-str}))
    ;; Otherwise, return no data
    (make-resp ctx {:type :rread
                    :data ""})))

(defn interop-dirreader
  [ctx qid]
  (if-let [child-qids (and (= (:type qid) proto/QTDIR)
                           (qid-children-qids (get-in ctx [:server-state :fs]) qid))]
    (let [buffer (io/default-buffer)
          stat-buffer (io/write-stats buffer (mapv #(fake-stat ctx %) child-qids) false)]
      (make-resp ctx {:type :rread
                      :data stat-buffer}))
    ;; Otherwise, return no data
    (make-resp ctx {:type :rread
                    :data ""})))

(defn hash-fs
  "We need to ensure the qids' paths are hashed (that they are all longs/numeric)"
  [base-state]
  (reduce
   (fn [new-fs [qid-map handle]]
     (assoc new-fs
            (with-meta (-> qid-map
                           (update :path io/path-hash)
                           (update :version (fnil identity 0)))
              {::string-path (str (:path qid-map))})
            handle))
   {}
   (:fs base-state)))

(defn- removev [pred coll] (vec (remove pred coll)))

(defn dispatch-handler [state handlers chans input-fcall]
   (let [thandler (handlers (:type input-fcall))
         ctx {:input-fcall  input-fcall
              :server-state state}]
     (async/go
       (try
         (thandler ctx)
         (catch Throwable t
           ((:ex-handler handlers reporting-ex-handler) t ctx))))))

(defn update-state-and-reply [state-atom chans channel rctx out-chan]
  (let [output-fcall (:output-fcall rctx
                                    (:output-fcall (rerror rctx "Server error: Nothing returned from handler.")))]
     (when-let [updater (:server-state-updater rctx)]
       (swap! state-atom updater))
     [(removev #{channel} chans) output-fcall]))

(defn server
  "Create a server given input and output channels,
  a map over override handlers, and an initial server
  state.  The input and output channels both expect only fcall maps.
  The call will start the server operating only on the channels,
  and return a 'server map' - containing the input and output channels,
  the handlers being used, and the state atom internal to the server."
  ([]
   (server default-initial-state {} (async/chan 10) (async/chan 10)))
  ([initial-state]
   (server initial-state {} (async/chan 10) (async/chan 10)))
  ([initial-state override-handlers]
   (server initial-state override-handlers (async/chan 10) (async/chan 10)))
  ([initial-state override-handlers in-chan out-chan]
   (let [handlers   (server-handlers override-handlers)
         base-state (n9p/deep-merge default-initial-state initial-state)
         state-atom (atom (assoc base-state :fs (hash-fs base-state)))]
     (assert (get-in base-state [:root :qid]) "Aborting: Server failed to establish a root qid")
     (async/go-loop [channels [in-chan]]
       (let [[value channel] (async/alts! channels)]
         (cond
           ;; value is input fcall map
           (and (= in-chan channel) (some? value))
           (recur (conj channels (dispatch-handler @state-atom handlers channels value)))

           ;; value is resulting context
           (some? value)
           (let [[channels reply] (update-state-and-reply state-atom channels channel value out-chan)]
             (async/>! out-chan reply)
             (recur channels))

           :else
           (async/close! out-chan))))
     {:server-in in-chan
      :server-out out-chan
      :handlers-9p handlers
      :state state-atom})))

(defn stop [serv-map]
  (when-let [server-in (:server-in serv-map)]
    (async/close! server-in)
    ;; Maybe we have a TCP server in hand
    (netty/stop serv-map)
    serv-map))

(defn netty-server
  ([channel-class server-options server-map-9p]
   ;; Start the Netty-specific output go-loop
   (async/go-loop [write-count 0]
     (if-let [output-fcall (async/<! (:server-out server-map-9p))]
       (do
         (.write ^ChannelHandlerContext (::remote output-fcall)
                 ;(io/encode-fcall! output-fcall (::buffer output-fcall)) ;; The buffer might be capped based on Framing
                 (io/encode-fcall! output-fcall (.directBuffer PooledByteBufAllocator/DEFAULT)))
         (if (or (>= write-count (:flush-every server-options 0))
                 (= :rflush (:type output-fcall)))
           (do (.flush ^ChannelHandlerContext (::remote output-fcall)) (recur 0))
           (recur (inc write-count))))
       (async/close! (:server-in server-map-9p))))
   ;; Build the Netty-based TCP server, and tie it 9p core.async server
   (merge (netty/server channel-class
                        server-options
                        [{:channel-registered (fn [^ChannelHandlerContext ctx]
                                                (let [ch ^Channel (.channel ctx)
                                                      pipeline ^ChannelPipeline (.pipeline ch)]
                                                  (.addFirst pipeline "Framer" (LengthFieldBasedFrameDecoder. ByteOrder/LITTLE_ENDIAN
                                                                                                              (:max-msg-size server-map-9p io/default-message-size-bytes)
                                                                                                              0 4 -4 0
                                                                                                              true))))
                          :channel-read (fn [^ChannelHandlerContext ctx msg]
                                          (let [buffer (cast ByteBuf msg)
                                                fcall (io/decode-fcall! (.duplicate buffer) {})]
                                            ;; Ensure backpressure bubbles up
                                            (when-not (async/>!! (:server-in server-map-9p)
                                                                 (assoc fcall
                                                                        ::buffer buffer
                                                                        ::remote ctx
                                                                        ::remote-addr (.. ctx (channel) (remoteAddress))))
                                              (.. ctx (channel) (close))
                                              (.. ctx (channel) (parent) (close)))))
                          ;; TODO: We need a way to retire the client tracking in the server-state
                          ;:disconnect (fn [^ChannelHandlerContext ctx p]
                          ;              (let [remote-addr (.. ctx (channel) (remoteAddress))]
                          ;                (println "TRYING to disconnect:" remote-addr)
                          ;                (swap! (:state server-map-9p) update-in [:client-fids] dissoc remote-addr)
                          ;                (.disconnect ^ChannelHandlerContext ctx ^ChannelPromise p)))
                          }])
          server-map-9p)))

(def tcp-server #(netty-server netty/tcp-channel-class %1 %2))
(def sctp-server #(netty-server netty/sctp-channel-class %1 %2))

(comment

  (def serv (server))
  (async/>!! (:server-in serv) (io/fcall {:type :tversion}))
  (async/<!! (:server-out serv))
  (async/close! (:server-in serv))


  ;; FS keys have to be qids to allow for multi-version
  (def serv (server {:ops {:stat stat-faker
                           :walk path-walker
                           :read clj-dirreader}
                     :fs {{:type proto/QTFILE :version 0
                           :path "/net"} {} ;; you can imagine this is a TCP stack
                          {:type proto/QTDIR :version 0
                           :path "/databases"} {:stat-info {:name "databases" ;; this is only to demo `:stat-info`
                                                            :mode (+ proto/DMDIR 0755)
                                                            :atime (io/now) :mtime (io/now)
                                                            :uid "dbuser" :gid "dbuser" :muid "dbuser"
                                                            :type 0 :dev 0 :statsz 0 :length 0}}
                          {:type proto/QTFILE :version 0
                           :path "/databases/some-db"} {:read (fn [context qid]
                                                                (make-resp context {:type :rread
                                                                                    :data "Hello world"}))
                                                        :open (fn [context qid])
                                                        :write (fn [context qid])
                                                        :walk (fn [context qid])}}}))

  (async/>!! (:server-in serv) (io/fcall {:type :tversion}))
  (async/>!! (:server-in serv) (io/fcall {:type :tattach :fid 1}))
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 1 :newfid 2 :wname ["net"]}))
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 2 :newfid 3 :wname ["net"]})) ;; Can't walk in non-dir
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 1 :newfid 3 :wname ["databases"]}))
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 3 :newfid 4 :wname ["some-db"]}))
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 3 :newfid 5 :wname ["no-db"]})) ;; Not found
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 1 :newfid 3 :wname ["net"]})) ;; Dup newfid
  (async/>!! (:server-in serv) (io/fcall {:type :twalk :fid 1 :newfid 5 :wname ["databases" "some-db"]}))
  (async/>!! (:server-in serv) (io/fcall {:type :tstat :fid 4}))
  (async/>!! (:server-in serv) (io/fcall {:type :tread :fid 5 :offset 0 :count 0}))
  (async/>!! (:server-in serv) (io/fcall {:type :tread :fid 3 :offset 0 :count 0}))
  (async/<!! (:server-out serv))
  (async/close! (:server-in serv))

  (def tcp-serv (tcp-server {:flush-every 0
                             :backlog 100
                             :reuseaddr true
                             :port 9090
                             :host "0.0.0.0"
                             :join? false}
                            serv))

  (netty/start tcp-serv)
  (-> serv :state deref :client-fids)

  )
