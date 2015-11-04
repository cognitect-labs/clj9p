(ns cognitect.clj9p.server-2
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

(def ^:dynamic *debug-fcall* true)
(def ^:dynamic *debug-reactor* nil)

(def removev (comp vec remove))

(def sconj (fnil conj #{}))
(def sdisj (fnil disj #{}))

(extend-protocol n9p/Remote
  nil
  (get-remote-id [t] t)

  ChannelHandlerContext
  (get-remote-id [t]
    (n9p/get-remote-id (.channel t)))

  Channel
  (get-remote-id [t]
    (.remoteAddress t)))

;; QidMapping m
;; ->qid :: m -> r -> Qid
;; qid-> :: m -> Qid -> r

;; Directory
;; lookup :: String -> Principal

;; Permission p
;; Principal = User u | Group g | Other
;; readable? :: Permission -> Principal -> Bool
;; writable? :: Permission -> Principal -> Bool
;; executable? :: Permission -> Principal -> Bool
;; user :: Permission -> User
;; group :: Permission -> Group
;; data operation = Read | Write | Execute
;; attempt :: (Permissioned r, Operation op) => r -> Principal -> op -> ( r -> x ) -> x | Error

;; Hierarchy h
;; root :: h -> e
;; child :: h -> e -> ( e -> Bool ) -> e
;; parent :: h -> e -> Maybe e
;; children :: h -> e -> [e]

;; class Permissioned r where
;;    permission :: r -> Permission

;; Filesystem fs
;; Resource r (type tbd by the fs)
;; random-access? :: r -> Bool
;; exclusive? :: r -> Bool
;; directory? :: r -> Bool
;; size :: r -> Long
;; modified :: r -> Instant
;; created :: r -> Instant
;; permission :: r -> Permission
;; read  :: r -> int -> buffer

;; change-permission :: FileSystem r -> r -> (FileSystem r, r)
;; create :: FileSystem r -> String -> (FileSystem r, r)
;; create-directory :: FileSystem r -> String -> (FileSystem r, r)
;; write :: FileSystem r -> r -> int -> buffer -> (FileSystem r, int)
;; truncate :: FileSystem r -> r -> (FileSystem r, r)
;; delete :: FileSystem r -> r -> (FileSystem r, r)

;; ExclusionRealm er
;; Exclusive e
;; lock :: ExclusionRealm -> Exclusive -> (ExclusionRealm, Lock e)
;; unlock :: Lock e -> ExclusionRealm

(defprotocol Handler
  (dispatch [this request] "Returns a channel that will deliver the result"))

(declare reporting-ex-handler)

(defn- remote-id    [request]        (n9p/get-remote-id (::remote request)))

(defn- hcontext-make
  [server-state client-state input-fcall]
  {:input-fcall input-fcall
   :server-state server-state
   :client-state client-state})

(defn- hcontext-lookup-fid   [context fid]    (-> context :client-state :fids (get fid)))

(def empty-client {:fids {}})

(defn- update-client-state!
  [clients remote ctx]
  (swap! clients assoc remote (:client-state ctx)))

(def ^:private mutator      :server-state-updater)
(def ^:private return       :output-fcall)

(defn- update-server-state!
  [server ctx]
  (swap! server (mutator ctx identity)))

(defrecord VirtualFileServer [server clients handlers]
  Handler
  (dispatch [this request]
    (let [remote (remote-id request)
          ctx    (hcontext-make @server (get @clients remote empty-client) request)]
      (try
        (let [ctx ((handlers (:type request)) ctx)]
          (update-client-state! clients remote ctx)
          (update-server-state! server ctx)
          ctx)
        (catch Throwable t
          (stacktrace/print-stack-trace t)
          ((:ex-handler handlers reporting-ex-handler) t ctx))))))

(defn op [context qid opcode]
  (get-in context [:server-state :fs qid opcode]
          (get-in context [:server-state :ops opcode])))

(defn server-root [context]
  (get-in context [:server-state :root :qid]))

(defn- fcall-request-id [input-fcall]
  [(n9p/get-remote-id (::remote input-fcall)) (:tag input-fcall)])

(defn- active-request-make [req-id channel]
  {:request-id req-id
   :channel    channel})

(defn- separate-by
  [pred coll]
  (let [grouped (group-by pred coll)]
    [(get grouped true) (get grouped false)]))

(defn reactor
  [request-channel reply-channel handler]
  (let []
    (async/go-loop [actives [(active-request-make nil request-channel)]]
      (let [[value channel] (async/alts! (mapv :channel actives))]
        (when *debug-reactor* (println "<< " value channel))
        (cond
          ;; new request arrived, dispatch and keep a channel for result
          (and (= request-channel channel) (some? value))
          (recur (conj actives
                       (active-request-make
                        (fcall-request-id value)
                        (async/go
                          (return
                           (dispatch handler value))))))

          ;; TODO - tflush needs to create this predicate and attach
          ;; it to the result value
          ;; need to flush some actives
          (some? (::flush value))
          (let [[keepers goners] (separate-by (::flush value) actives)]
            (loop [channels (map :channel goners)]
              (when-let [ch (seq channels)]
                (async/close! ch)
                (recur (next channels))))
            (recur keepers))

          ;; result delivered
          (some? value)
          (do (async/>! reply-channel value)
              (recur (removev #(= channel (:channel %)) actives)))

          ;; request-channel was closed
          :else
          (async/close! reply-channel))))))

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
  (println "Error happened with fcall:" (:input-fcall ctx) "\n\t" ename)
  (make-resp ctx {:type :rerror :ename ename}))

(defn unknown-fid
  [ctx fid]
  (println "Fcall for unknown fid" (:input-fcall ctx))
  (rerror ctx (str "Unknown fid: " fid)))

(defn directory? [qid]
  (pos? (bit-and (:type qid) proto/QTDIR)))

(defn- client-assign-fid [client-state fid uname qid]
  (assoc-in client-state [:fids fid] {:uname     uname
                                      :qid       qid
                                      :open-mode -1
                                      ;; TODO: Handle auth cases
                                      :auth?     false}))

(defn- client-open-fid [client-state fid mode]
  (update-in client-state [:fids fid] assoc :open-mdoe mode))

(defn- client-unassign-fid [client-state fid]
  (update-in client-state [:fids] dissoc fid))


;; Response functions/handlers
;; ---------------------------

(defn tversion
  [context]
  (let [request-version (get-in context [:input-fcall :version])
        version (if (.startsWith ^String request-version "9P2000")
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
        fid               (:fid input-fcall)
        client-fid        (hcontext-lookup-fid context fid)
        {:keys [fs root]} (:server-state context)
        attach-fn         (get-in context [:server-state :ops :attach])
        fid               (:fid input-fcall)
        root-qid          (:qid root)]
    ;; TODO: Add afid handling
    (cond
      client-fid (rerror context (str "Duplicate fid for client: " fid))
      attach-fn  (attach-fn context {})
      :else      (-> context
                     (update :client-state client-assign-fid fid (:uname input-fcall) root-qid)
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
        input-fid    (:fid input-fcall)
        input-newfid (:newfid input-fcall)
        fid          (hcontext-lookup-fid context input-fid)
        newfid       (hcontext-lookup-fid context input-newfid)
        qid          (:qid fid)
        root-qid     (server-root context)
        file-walk    (op context qid :walk)]
    (cond
      (nil? fid)
      (unknown-fid context input-fid)

      (not= (:open-mode fid) -1)
      (do (println "fid is " fid)
          (rerror context "Cannot clone an open fid"))

      (and (pos? (count (:wname input-fcall)))
           (not (directory? qid)))
      (rerror context "Cannot walk in non-directory")

      (and (not= input-fid input-newfid) newfid)
      (rerror context (str "newfid is a duplicate fid for client: " input-newfid))

      (zero? (count (:wname input-fcall)))
      (-> context
          (update :client-state client-assign-fid input-newfid (:uname input-fcall "") root-qid)
          (make-resp {:type :rwalk
                      :wqid []}))

      file-walk
      (file-walk context qid)

      :else
      (rerror context "No walk function"))))

(defn topen
  [context]
  (let [input-fcall (:input-fcall context)
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        root-qid    (server-root context)
        file-open   (op context qid :open)]
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
          (update :client-state client-open-fid input-fid (:mode input-fcall))
          (make-resp {:type   :ropen
                      :qid    qid
                      :iounit (- ^long (:iounit input-fcall) ^long proto/IOHDRSZ)})))))

(defn tcreate
  [context]
  (let [input-fcall (:input-fcall context)
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        file-create (op context qid :create)]
    (cond
      (nil? fid)                     (unknown-fid context input-fid)
      (not= (:open-mode fid) -1)     (rerror context "Botched 9P call: Cannot create in a non-open'd descriptor")

      (not (directory? qid))         (rerror context "Cannot create in a non-directory")
      file-create                    (file-create context qid)
      :else                          (rerror context "No create function"))))

(defn tread
  [context]
  (let [input-fcall (:input-fcall context)
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        read-count  (:count input-fcall)
        read-count  (if (> ^long read-count (- ^long (:msize input-fcall) ^long proto/IOHDRSZ))
                      (- ^long (:msize input-fcall) ^long proto/IOHDRSZ)
                      read-count)
        file-read   (op context qid :read)]
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
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        write-count (:count input-fcall (count (:data input-fcall)))
        write-count (if (> ^long write-count ^long (- ^long (:msize input-fcall) ^long proto/IOHDRSZ))
                      (- ^long (:msize input-fcall) ^long proto/IOHDRSZ)
                      write-count)
        file-write  (op context qid :write)]
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
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        file-clunk  (op context qid :clunk)]
    (cond
      (nil? fid) (unknown-fid context input-fid)
      file-clunk (file-clunk context qid)
      ;; There is no rclunk support (not needed); Drop the fid
      :else      (-> context
                     (update :client-state client-unassign-fid input-fid)
                     (make-resp {:type :rclunk})))))

(defn tremove
  [context]
  (let [input-fcall (:input-fcall context)
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        file-remove (op context qid :remove)]
    (cond
      (nil? fid)  (unknown-fid context input-fid)
      file-remove (file-remove context qid)
      ;; There is no rremove support (not needed)
      :else       (update context :client-state client-unassign-fid input-fid))))

(defn tstat
  [context]
  (let [input-fcall (:input-fcall context)
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        file-stat   (op context qid :stat)
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
        input-fid   (:fid input-fcall)
        fid         (hcontext-lookup-fid context input-fid)
        qid         (:qid fid)
        file-wstat  (op context qid :wstat)]
    (cond
      (nil? fid) (unknown-fid context input-fid)
      file-wstat (file-wstat context qid)
      :else      (rerror context "No wstat function"))))

(defn reporting-ex-handler [^Throwable t ctx]
  (rerror ctx (str "There was a server error when handling the " (get-in ctx [:input-fcall :type])
                   "msg.\nReason: " t)))

(def default-handlers
  {:tversion   tversion
   :tauth      tauth
   :tattach    tattach
   :tflush     tflush
   :twalk      twalk
   :topen      topen
   :tcreate    tcreate
   :tread      tread
   :twrite     twrite
   :tclunk     tclunk
   :tremove    tremove
   :tstat      tstat
   :twstat     twstat
   ;; The `ex-handler` is used to rescue bad requests and produce valid r-messages that report the error
   :ex-handler reporting-ex-handler})

(defn server-handlers [override-map]
  (merge default-handlers
         override-map))

(def default-initial-state {;; The root stat of your filesystem
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

(defn stub-output-fn [output-fcall]
  (fn [ctx qid]
    (assoc ctx
           :output-fcall (merge (:input-fcall ctx)
                                output-fcall))))

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
  (subs path 0 (.lastIndexOf ^String path "/")))

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
        {:keys [wname newfid]} input-fcall
        input-fid              (:fid input-fcall)
        fid                    (hcontext-lookup-fid ctx input-fid)
        root-qid               (server-root ctx)
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
          (update :client-state client-assign-fid newfid (:uname fid) (or (last wqid) root-qid))
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
  (if-let [child-qids (and (directory? qid)
                           (qid-children-qids (get-in ctx [:server-state :fs]) qid))]
    (let [buffer (io/little-endian (io/default-buffer))
          stat-buffer (io/write-stats buffer (mapv #(fake-stat ctx %) child-qids) false)
          ;; TODO: offset slice isn't enough, it also needs to factor in the length of the read i-fcall
          ret-buffer (io/slice stat-buffer (get-in ctx [:input-fcall :offset]))]
      (make-resp ctx {:type :rread
                      :data ret-buffer}))
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
         state-atom (atom (assoc base-state :fs (hash-fs base-state)))
         vfs-server (->VirtualFileServer state-atom (atom {}) handlers)]
     (assert (get-in base-state [:root :qid]) "Aborting: Server failed to establish a root qid")
     (reactor in-chan out-chan vfs-server)
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

(def fcall-keys
  {:tversion [:type :tag :msize :version]
   :rversion [:type :tag :msize :version]
   :tauth    [:type :tag :afid :uname :aname]
   :rauth    [:type :tag :aqid]
   :rerror   [:type :tag :ename]
   :tflush   [:type :tag :oldtag]
   :rflush   [:type :tag]
   :tattach  [:type :tag :afid :uname :aname]
   :rattach  [:type :tag :qid]
   :twalk    [:type :tag :fid :newfid :wname]
   :rwalk    [:type :tag :wqid]
   :topen    [:type :tag :fid :mode]
   :ropen    [:type :tag :qid :iounit]
   :topenfd  [:type :tag :fid :mode]
   :ropenfd  [:type :tag :qid :iounit :unixfd]
   :tcreate  [:type :tag :fid :name :perm :mode]
   :rcreate  [:type :tag :qid :iounit]
   :tread    [:type :tag :fid :offset :count]
   :rread    [:type :tag :count :data]
   :twrite   [:type :tag :fid :offset :count :data]
   :rwrite   [:type :tag :count]
   :tclunk   [:type :tag :fid]
   :rclunk   [:type :tag]
   :tremove  [:type :tag :fid]
   :rremove  [:type :tag]
   :tstat    [:type :tag :fid]
   :rstat    [:type :tag :stat]
   :twstat   [:type :tag :fid :stat]
   :rwstat   [:type :tag]})

(defn- show-fcall
  [fcall-map]
  (pr-str (select-keys fcall-map (get fcall-keys (:type fcall-map) [:type :tag]))))

(defn netty-server
  ([channel-class server-options server-map-9p]
   ;; Start the Netty-specific output go-loop
   (async/go-loop [write-count 0]
     (if-let [output-fcall (async/<! (:server-out server-map-9p))]
       (do
         (when *debug-fcall* (println "<- " (show-fcall output-fcall)))
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
                                            (when *debug-fcall* (println "-> " (show-fcall fcall)))
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
