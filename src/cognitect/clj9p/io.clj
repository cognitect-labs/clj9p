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

(ns cognitect.clj9p.io
  (:require [cognitect.clj9p.9p :as n9p]
            [cognitect.clj9p.proto :as proto]
            [cognitect.clj9p.buffer :as buff]
            [clojure.core.async :as async])
  (:import (cognitect.clj9p.buffer Buffer)
           (java.nio ByteOrder)
           (io.netty.buffer ;Unpooled
                            ByteBuf
                            ByteBufUtil)
           (io.netty.buffer PooledByteBufAllocator)
           (java.nio ByteBuffer)
           (java.nio.channels ByteChannel)
           (io.netty.channel ChannelHandlerContext)
           (java.util Date)))

;; 9P Protocol implementation notes
;; --------------------------------

;; At the bottom level, 9P operates on calls sent to file descriptors (files, sockets, channels).
;; In this implementation, calls are constructed using Clojure maps.
;; Many benefits fall out of this choice (they can be serialized with JSON or
;; Transit, they can be stored on a file or database and read in, etc.).  When
;; a message/call is sent to the fd, it is encoded into binary and packed on a
;; buffer.  Buffers are programmed against an interface and the default
;; implementation uses a Netty ByteBuf (DirectBuffer).  Buffers are also used
;; to unpack calls/messages and turn them back into maps.

(defn default-buffer
  "This creates a Buffer suitable for use with encoding/decoding fcalls,
  but one should prefer using PooledByteBufAllocator from Netty (with direct Buffers only)"
  ([]
   (buff/ensure-little-endian (.directBuffer PooledByteBufAllocator/DEFAULT)))
  ([initial-byte-array]
   (if (instance? ByteBuf initial-byte-array)
     (buff/ensure-little-endian initial-byte-array) ;; Caller assumed Byte Arrays, but was operating in-place
     (buff/write-byte-array (buff/ensure-little-endian (.directBuffer PooledByteBufAllocator/DEFAULT)) initial-byte-array))))

(extend-protocol buff/Length

  ByteBuf
  (length [t]
    (.readableBytes t))

  String
  (length [t]
    (.length t))

  nil
  (length [t] 0))

(extend-protocol buff/Buffer

  ByteBuf
  (write-string [t s]
    (ByteBufUtil/writeUtf8 t ^String s)
    t)
  (write-byte-array [t ba]
    (.writeBytes t ^bytes ba))
  (write-bytes [t bb]
    (.writeBytes t ^ByteBuf bb)
    t)
  (write-nio-bytes [t byte-buffer]
    (.writeBytes t ^ByteBuffer byte-buffer))
  (write-zero [t n]
    (.writeZero t n)
    t)
  (write-byte [t b]
    (.writeByte t b)
    t)
  (write-short [t s]
    (.writeShort t s)
    t)
  (write-int [t i]
    (.writeInt t i)
    t)
  (write-long [t l]
    (.writeLong t l)
    t)
  (writer-index [t]
    (.writerIndex t))
  (move-writer-index [t n]
    (.writerIndex t n)
    t)
  (read-string [t n]
    (let [ba (byte-array n)]
      (.readBytes t ba)
      (String. ba "UTF-8")))
  (read-bytes [t n]
    (let [ba (byte-array n)]
      (.readBytes t ba)
      ba))
  (read-byte [t]
    (.readUnsignedByte t))
  (read-short [t]
    (.readUnsignedShort t))
  (read-int [t]
    (.readUnsignedInt t))
  (read-long [t]
    (.readLong t)) ;; TODO: Should the appropriate shift be done here
  (readable-bytes [t]
    (.readableBytes t))
  (reset-read-index [t]
    (.readerIndex t 0)
    t)
  (as-byte-array [t]
    (.array t))
  (as-byte-buffer [t]
    (.nioBuffer t)) ;; Note: This will share content with the underlying Buffer
  (ensure-little-endian [t]
    (.order t ByteOrder/LITTLE_ENDIAN))
  (slice [t index length]
    (.slice t index length))
  (clear [t]
    (.clear t)
    t))

(defn slice
  ([t offset]
  (buff/slice t offset (- (buff/length t) offset)))
  ([t offset length]
   (buff/slice t offset length)))

(def little-endian buff/ensure-little-endian)

(def length buff/length)

;; Auxiliary functions
;; -------------------

(defn path-hash [path]
  (if (string? path)
    (Math/abs ^int (hash path))
    path))

(defn write-qid [^Buffer buffer qid-map]
  (buff/write-byte buffer (:type qid-map))
  (buff/write-int buffer (:version qid-map))
  (buff/write-long buffer (path-hash (:path qid-map)))
  buffer)

(defn read-qid [^Buffer buffer]
  {:type (buff/read-byte buffer)
   :version (buff/read-int buffer)
   :path (buff/read-long buffer)})

;; 9P Doesn't allow for any NUL characters at all.
;; Instead of null-terminated strings, all free-form data is prepended
;; by the length of bytes to read - as a short [2] or int [4].
;; This design choice means you can always safely read 9P "front" to "back".
;; All strings are always UTF-8 (originally designed for Plan 9 and the 9P protocol).

(defn write-2-piece [^Buffer buffer x]
  (buff/write-short buffer (buff/length x))
  (if (string? x)
    (buff/write-string buffer x)
    (buff/write-bytes buffer x))
  buffer)
(def write-len-string write-2-piece)

(defn write-4-piece [^Buffer buffer x]
  (buff/write-int buffer (buff/length x))
  (if (string? x)
    (buff/write-string buffer x)
    (buff/write-bytes buffer x))
  buffer)

(defn read-2-piece [^Buffer buffer]
  (let [n (buff/read-short buffer)]
    (buff/read-bytes buffer n)))

(defn read-len-string [^Buffer buffer]
  (let [n (buff/read-short buffer)]
    (buff/read-string buffer n)))

(defn read-4-piece [^Buffer buffer]
  (let [n (buff/read-int buffer)]
    (buff/read-bytes buffer n)))

(defn write-stats [^Buffer buffer stats encode-length?]
  (let [buffer (buff/ensure-little-endian buffer)
        statsz-processed (reduce (fn [{:keys [stats statsz-total]} {:keys [name uid gid muid extension] :as stat}]
                                   (let [stat-sz (apply + (if extension 61 47) ;; base size values for standard vs dot-u
                                                        (if extension (count extension) 0) ;; If we're :dot-u, we'll have an extension
                                                        (map count [name uid gid muid]))]
                                     {:stats (conj stats (assoc stat :statsz stat-sz))
                                      :statsz-total (+ ^long statsz-total ^long stat-sz)}))
                                 {:stats []
                                  :statsz-total 0} stats)]
    (when encode-length?
      (buff/write-short buffer (+ ^long (:statsz-total statsz-processed) 2)))
    (doseq [stat (:stats statsz-processed)]
      (buff/write-short buffer (:statsz stat))
      (buff/write-short buffer (:type stat))
      (buff/write-int buffer (:dev stat))
      (write-qid buffer (:qid stat))
      (buff/write-int buffer (:mode stat))
      (buff/write-int buffer (:atime stat))
      (buff/write-int buffer (:mtime stat))
      (buff/write-long buffer (:length stat))
      (write-len-string buffer (:name stat))
      (write-len-string buffer (:uid stat))
      (write-len-string buffer (:gid stat))
      (write-len-string buffer (:muid stat))
      (when (:extension stat) ;; when dot-u
        (write-len-string buffer (:extension stat))
        (buff/write-int buffer (:uidnum stat))
        (buff/write-int buffer (:gidnum stat))
        (buff/write-int buffer (:muidnum stat))))
    buffer))

(defn read-stats [^Buffer buffer decode-length? dot-u?]
  (when decode-length?
    (buff/read-short buffer))
  (loop [stats (transient [])
         initial-bytes ^long (buff/readable-bytes buffer)]
    (if (pos? initial-bytes)
      (let [statsz (buff/read-short buffer)
            stype (buff/read-short buffer)
            dev (buff/read-int buffer)
            qid (read-qid buffer)
            mode (buff/read-int buffer)
            atime (buff/read-int buffer)
            mtime (buff/read-int buffer)
            slen (buff/read-long buffer)
            sname (read-len-string buffer)
            uid (read-len-string buffer)
            gid (read-len-string buffer)
            muid (read-len-string buffer)
            ;;(if (or dot-u?
            ;;                  (> statsz (- initial-bytes (buff/readable-bytes buffer)))
            ;;           (read-len-string buffer)
            ;;           ""))
            extension (if dot-u? (read-len-string buffer) "")
            uidnum (if dot-u? (buff/read-int buffer) 0)
            gidnum (if dot-u? (buff/read-int buffer) 0)
            muidnum (if dot-u? (buff/read-int buffer) 0)]
        (recur (conj! stats (merge {:statsz statsz
                                    :type stype
                                    :dev dev
                                    :qid qid
                                    :mode mode
                                    :atime atime
                                    :mtime mtime
                                    :length slen
                                    :name sname
                                    :uid uid
                                    :gid gid
                                    :muid muid}
                                   (when dot-u?
                                     {:extension extension
                                      :uidnum uidnum
                                      :gidnum gidnum
                                      :muidnum muidnum})))
               ^long (buff/readable-bytes buffer)))
      (persistent! stats))))

(defn ensure!
  ([pred x]
   (ensure! pred x (str "Value failed predicate contract: " x)))
  ([pred x message]
   (if (pred x)
    x
    (throw (ex-info message {:type :ensure-violation
                             :predicate pred
                             :value x})))))

(defn now [] (.getTime ^Date (Date.)))

(def mode-bits ["---" "--x" "-w-" "-wx" "r--" "r-x" "rw-" "rwx"])
(defn mode-str [mode-num]
  (let [mode-fn (fn [shift-num]
                  ; Just numbers...
                  ;(bit-and (bit-shift-right mode-num shift-num) 7)
                  (get mode-bits (bit-and (bit-shift-right mode-num shift-num) 7)))
        mode-bit (cond
                   (pos? (bit-and mode-num proto/DMDIR)) "d"
                   (pos? (bit-and mode-num proto/DMAPPEND)) "a"
                   :else "-")]
    (str mode-bit (mode-fn 6) (mode-fn 3) (mode-fn 0))))

(def default-message-size-bytes 8192)

(defn numeric-fcall
  "Replace all fcall map values with the ensured numeric values for the protocol"
  [fcall-map]
  (merge fcall-map
         {:type (let [call-type (:type fcall-map)]
                  (ensure! proto/v2-codes
                           (if (keyword? call-type)
                             (proto/v2 call-type)
                             call-type)
                           (str "Invalid type value: " call-type)))
          :tag (ensure! number?  (:tag fcall-map)
                        (str "Invalid tag value: " (:tag fcall-map)))}))

(defn valid-fcall? [fcall-map]
  (every? identity (some-> fcall-map
                           (select-keys [:type :tag])
                           vals)))

(defn fcall
  "Create a valid fcall-map with defaults set.
  Can accept a base map, or kv-args"
  ([base-map]
   {:pre [(map? base-map)]
    :post [valid-fcall?]}
   (merge {:tag (rand-int 32000) ;; Random short generation takes ~0.05ms
           :fid proto/NOFID
           :afid proto/NOFID
           :uname (System/getProperty "user.name")
           :aname ""
           :stat []
           :iounit default-message-size-bytes
           :wqid []
           :msize default-message-size-bytes ;; Can be as large as 65512
           :version (if (:dot-u base-map)
                      proto/versionu
                      proto/version)
           :uidnum proto/UIDUNDEF
           :gidnum proto/UIDUNDEF
           :muidnum proto/UIDUNDEF}
          base-map))
  ([k & vkv-pairs]
   {:pre [(keyword? k)]}
   (fcall (apply hash-map k vkv-pairs))))

(defn stat
  "Generate a close-to-complete stat map"
  [base-map]
  (merge {:type 0
          :dev 0
          :statsz 0
          :mode 0644
          :length 0 ;; 0 Length is used for directories and devices/services
          :name ""
          :uid "user"
          :gid "user"
          :muid "user"}
         base-map))

;; Encoding and decoding
;; ----------------------
;; Messages are defined in the [RFC Message Section](http://9p.cat-v.org/documentation/rfc/#msgs)
;; fcalls/message are based on the following struct:
;;   type Fcall struct {
;;       Size    uint32   // size of the message (4 - int)
;;       Type    uint8    // message type (1 - byte)
;;       Fid     uint32   // file identifier (4 - int)
;;       Tag     uint16   // message tag (2 - short)
;;       Msize   uint32   // maximum message size (used by Tversion, Rversion) (4 - int)
;;       Version string   // protocol version (used by Tversion, Rversion) (string)
;;       Oldtag  uint16   // tag of the message to flush (used by Tflush) (2 - short)
;;       Error   string   // error (used by Rerror) (string)
;;       Qid              // file Qid (used by Rauth, Rattach, Ropen, Rcreate) (struct - 1, 4, 8 - type, version, path)
;;       Iounit  uint32   // maximum bytes read without breaking in multiple messages (used by Ropen, Rcreate) (4 - int)
;;       Afid    uint32   // authentication fid (used by Tauth, Tattach) (4 - int)
;;       Uname   string   // user name (used by Tauth, Tattach) (string)
;;       Aname   string   // attach name (used by Tauth, Tattach) (string)
;;       Perm    uint32   // file permission (mode) (used by Tcreate) (4 - int)
;;       Name    string   // file name (used by Tcreate) (string)
;;       Mode    uint8    // open mode (used by Topen, Tcreate) (1 - byte)
;;       Newfid  uint32   // the fid that represents the file walked to (used by Twalk) (4 - int)
;;       Wname   []string // list of names to walk (used by Twalk) (vector of strings)
;;       Wqid    []Qid    // list of Qids for the walked files (used by Rwalk) (vector of Qids)
;;       Offset  uint64   // offset in the file to read/write from/to (used by Tread, Twrite) (8 - long)
;;       Count   uint32   // number of bytes read/written (used by Tread, Rread, Twrite, Rwrite) (4 - int)
;;       Dir              // file description (used by Rstat, Twstat)
;;
;;       /* 9P2000.u extensions */
;;       Errornum  uint32 // error code, 9P2000.u only (used by Rerror) (4 - int)
;;       Extension string // special file description, 9P2000.u only (used by Tcreate) (string)
;;       Unamenum  uint32 // user ID, 9P2000.u only (used by Tauth, Tattach) (4 - int)
;;   }

(def valid-fcall-keys #{:msize       ; Tversion, Rversion
                        :version     ; Tversion, Rversion
                        :oldtag      ; Tflush
                        :ename       ; Rerror
                        :qid         ; Rattach, Ropen, Rcreate
                        :iounit      ; Ropen, Rcreate
                        :aqid        ; Rauth
                        :afid        ; Tauth, Tattach
                        :uname       ; Tauth, Tattach
                        :aname       ; Tauth, Tattach
                        :perm        ; Tcreate
                        :name        ; Tcreate
                        :mode        ; Tcreate, Topen
                        :newfid      ; Twalk
                        :wname       ; Twalk, array
                        :wqid        ; Rwalk, array
                        :offset      ; Tread, Twrite
                        :count       ; Tread, Twrite
                        :data        ; Twrite, Rread
                        :nstat       ; Twstat, Rstat
                        :stat        ; Twstat, Rstat
                        ; dotu extensions:
                        :errno       ; Rerror
                        ;              Tcreate
                        :extension})

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

(defn show-fcall
  [fcall-map]
  (pr-str (select-keys fcall-map (get fcall-keys (:type fcall-map) [:type :tag]))))

;; 9P has two types of messages: T-Messages and R-Messages.
;; From the RFC/Intro:
;;  The Plan 9 File Protocol, 9P, is used for messages between clients and servers.
;;  A client transmits *requests* (T-messages) to a server, which subsequently returns *replies* (R-messages) to the client.
;;  The combined acts of transmitting (receiving) a request of a particular type, and receiving (transmitting) its reply
;;    is called a transaction of that type.
;;  Each message consists of a sequence of bytes. Two-, four-, and eight-byte fields
;;  hold unsigned integers represented in little-endian order  (least significant byte first).
;;  Data items of larger or variable lengths are represented by a two-byte field specifying a count, n, followed by n bytes of data.
;;  Text strings are represented this way, with the text itself stored as a UTF-8 encoded sequence of Unicode characters.
;;  Text strings in 9P messages are not NUL-terminated: n counts the bytes of UTF-8 data, which include no final zero byte.
;;  The NUL character is illegal in all text strings in 9P, and is therefore excluded from file names, user names, and so on.
;;  Messages are transported in byte form to allow for machine independence.

(defn encode-fcall!
  "Given an fcall map and a Buffer,
  encode the fcall onto the buffer and return the buffer

  NOTE: This will reset/clear the buffer"
  [fcall-map ^Buffer buffer]
  (let [;; Return a little endian buffer
        buffer (buff/ensure-little-endian buffer)
        fcall (numeric-fcall fcall-map)
        ;; We'll match the type based on a keyword...
        fcall-type (if (keyword? (:type fcall-map))
                     (:type fcall-map)
                     (proto/v2-codes (:type fcall)))]
    (buff/clear buffer)
    ;; TODO: This may have to be `write-int 0`
    (buff/write-zero buffer 4) ; "0000" - which will be used for total msg len when we're done
    ;; Imperatively pack the buffer
    (buff/write-byte buffer (:type fcall))
    (buff/write-short buffer (:tag fcall))
    ;; TODO: Consider replacing this cond with a dispatch map
    (cond
      (#{:tversion :rversion} fcall-type) (do (buff/write-int buffer (:msize fcall))
                                              (write-len-string buffer (:version fcall)))
      (= fcall-type :tauth) (do (buff/write-int buffer (:afid fcall))
                                (write-len-string buffer (:uname fcall))
                                (write-len-string buffer (:aname fcall))
                                (when (:dot-u fcall)
                                  (buff/write-int buffer (:uidnum fcall))))
      (= fcall-type :rauth) (write-qid buffer (:aqid fcall))
      (= fcall-type :rerror) (do (write-len-string buffer (:ename fcall "General Error"))
                                 (when (:dot-u fcall)
                                   (buff/write-int buffer (:errno fcall))))
      (= fcall-type :tflush) (buff/write-short buffer (:oldtag fcall))
      (= fcall-type :tattach) (do (buff/write-int buffer (:fid fcall))
                                  (buff/write-int buffer (:afid fcall))
                                  (write-len-string buffer (:uname fcall))
                                  (write-len-string buffer (:aname fcall))
                                  (when (:dot-u fcall)
                                    (buff/write-int buffer (:uidnum fcall))))
      (= fcall-type :rattach) (write-qid buffer (:qid fcall))
      (= fcall-type :twalk) (do (buff/write-int buffer (:fid fcall))
                                (buff/write-int buffer (:newfid fcall))
                                (buff/write-short buffer (count (:wname fcall)))
                                (doseq [node-name (:wname fcall)]
                                  (write-len-string buffer node-name)))
      (= fcall-type :rwalk) (do (buff/write-short buffer (count (:wqid fcall)))
                                (doseq [qid (:wqid fcall)]
                                  (write-qid buffer qid)))
      (= fcall-type :topen) (do (buff/write-int buffer (:fid fcall))
                                (buff/write-byte buffer (:mode fcall)))
      (#{:ropen :rcreate} fcall-type) (do (write-qid buffer (:qid fcall))
                                          (buff/write-int buffer (:iounit fcall)))
      (= fcall-type :tcreate) (do (buff/write-int buffer (:fid fcall))
                                  (write-len-string buffer (:name fcall))
                                  (buff/write-int buffer (:perm fcall))
                                  (buff/write-byte buffer (:mode fcall))
                                  (when (:dot-u fcall)
                                    (write-len-string (:extension fcall))))
      (= fcall-type :tread) (do (buff/write-int buffer (:fid fcall))
                                (buff/write-long buffer (:offset fcall))
                                (buff/write-int buffer (:count fcall)))
      (= fcall-type :rread) (write-4-piece buffer (:data fcall))
      (= fcall-type :twrite) (do (buff/write-int buffer (:fid fcall))
                                (buff/write-long buffer (:offset fcall))
                                (write-4-piece buffer (:data fcall)))
      (= fcall-type :rwrite) (buff/write-int buffer (:count fcall))
      (#{:tclunk :tremove :tstat} fcall-type) (buff/write-int buffer (:fid fcall))
      (#{:rstat :twstat} fcall-type) (do (when (= fcall-type :twstat)
                                           (buff/write-int buffer (:fid fcall)))
                                         (write-stats buffer (:stat fcall) true))
      :else buffer)

  ;; Patch up the size of the message
  (let [length (buff/length buffer)]
    (buff/move-writer-index buffer 0)
    (buff/write-int buffer length)
    (buff/move-writer-index buffer length))))

(def ^{:doc "Return a transducer that encodes an fcall into a Buffer"}
  encode-fcall-xf
  (map
   (fn [fcall]
     (encode-fcall! fcall (.directBuffer PooledByteBufAllocator/DEFAULT)))))

(defn decode-fcall!
  "Given a buffer (full of bytes) and optionally a base fcall map,
  decode the bytes into a full fcall map.  Returns the decoded/complete fcall-map.

  Note: This resets the read index on the buffer on entry and exhausts it by exit."
  ([^Buffer base-buffer]
   (decode-fcall! base-buffer {}))
  ([^Buffer base-buffer base-fcall-map]
  (let [buffer (buff/ensure-little-endian base-buffer)
        _ (buff/reset-read-index buffer)
        buffer-size (buff/length buffer)
        size (buff/read-int buffer)
        _ (when (not= buffer-size size)
            (throw (ex-info (str "Reported message size and actual size differ: " size "; actual: " buffer-size)
                            {:reported-size size
                             :buffer-size buffer-size
                             :fcall-map base-fcall-map})))
        _ (when-not (<= 7 size 0xffffffff)
            (throw (ex-info (str "Bad fcall message size on decode: " size)
                            {:size size
                             :fcall-map base-fcall-map})))
        ftype (buff/read-byte buffer)
        ftag (buff/read-short buffer)
        fcall-type (ensure! identity
                            (proto/v2-codes ftype)
                            (str "Decoded invalid type numeric: " ftype))
        fcall-map (fcall {:type fcall-type
                          :tag ftag
                          :original-size size})]
    ;; TODO: Consider replacing this cond with a dispatch map
    (cond
      (#{:tversion :rversion} fcall-type) (assoc fcall-map
                                                 :msize (buff/read-int buffer)
                                                 :version (read-len-string buffer))
      (= fcall-type :tauth) (let [afid (buff/read-int buffer)
                                  uname (read-len-string buffer)
                                  aname (read-len-string buffer)]
                             (merge fcall-map
                                   {:afid afid
                                    :uname uname
                                    :aname aname}
                                   (when (:dot-u fcall-map)
                                     {:uidnum (buff/read-int buffer)})))
      (= fcall-type :rauth) (assoc fcall-map
                                   :aqid (read-qid buffer))
      (= fcall-type :rerror) (merge fcall-map
                                    {:ename (read-len-string buffer)}
                                    (when (:dot-u fcall-map)
                                     {:errno (buff/read-int buffer)}))
      (= fcall-type :tflush) (assoc fcall-map
                                    :oldtag (buff/read-short buffer))
      (= fcall-type :tattach) (let [fid (buff/read-int buffer)
                                    afid (buff/read-int buffer)
                                    uname (read-len-string buffer)
                                    aname (read-len-string buffer)]
                                (merge fcall-map
                                       {:fid fid
                                        :afid afid
                                        :uname uname
                                        :aname aname}
                                       (when (:dot-u fcall-map)
                                         {:uidnum (buff/read-int buffer)})))
      (= fcall-type :rattach) (assoc fcall-map
                                     :qid (read-qid buffer))
      (= fcall-type :twalk) (let [fid (buff/read-int buffer)
                                  newfid (buff/read-int buffer)
                                  n (buff/read-short buffer)]
                              (assoc fcall-map
                                   :fid fid
                                   :newfid newfid
                                   :wname (mapv (fn [name-n]
                                                  (read-len-string buffer)) (range n))))
      (= fcall-type :rwalk) (let [n (buff/read-short buffer)]
                              (assoc fcall-map
                                     :wqid (mapv (fn [name-n] (read-qid buffer)) (range n))))
      (= fcall-type :topen) (assoc fcall-map
                                   :fid (buff/read-int buffer)
                                   :mode (buff/read-byte buffer))
      (#{:ropen :rcreate} fcall-type) (assoc fcall-map
                                             :qid (read-qid buffer)
                                             :iounit (buff/read-int buffer))
      (= fcall-type :tcreate) (let [fid (buff/read-int buffer)
                                    name (read-len-string buffer)
                                    perm (buff/read-int buffer)
                                    mode (buff/read-byte buffer)]
                                (merge fcall-map
                                     {:fid fid
                                      :name name
                                      :perm perm
                                      :mode mode}
                                     (when (:dot-u fcall-map)
                                       {:extension (read-len-string buffer)})))
      (= fcall-type :tread) (assoc fcall-map
                                   :fid (buff/read-int buffer)
                                   :offset (buff/read-long buffer)
                                   :count (buff/read-int buffer))
      (= fcall-type :rread) (assoc fcall-map
                                   :data (read-4-piece buffer))
      (= fcall-type :twrite) (let [fid (buff/read-int buffer)
                                   offset (buff/read-long buffer)
                                   cnt (buff/read-int buffer)]
                               (assoc fcall-map
                                      :fid fid
                                      :offset offset
                                      :count cnt
                                      :data (buff/read-bytes buffer cnt)))
      (= fcall-type :rwrite) (assoc fcall-map
                                    :count (buff/read-int buffer))
      (#{:tclunk :tremove :tstat} fcall-type) (assoc fcall-map
                                                     :fid (buff/read-int buffer))
      (#{:rstat :twstat} fcall-type) (merge fcall-map
                                            (when (= fcall-type :twstat)
                                              {:fid (buff/read-int buffer)})
                                            {:stat (read-stats buffer true (:dot-u fcall-map))})
      :else fcall-map))))

(def ^{:doc "A transducer that converts Buffers into fcall maps"}
  decode-fcall-xf
  (map
   (fn [^Buffer buffer]
     (decode-fcall! buffer))))

(defn directory? [qid-type]
  (if (map? qid-type)
    (directory? (:type qid-type))
    (pos? (bit-and qid-type proto/QTDIR))))

(defn file? [qid-type]
  (if (map? qid-type)
    (file? (:type qid-type))
    (or (= qid-type proto/QTFILE) ;; Protect against mode bit '0'
        (pos? (bit-and qid-type proto/QTFILE))
        (pos? (bit-and qid-type proto/QTAPPEND))
        (pos? (bit-and qid-type proto/QTTMP)))))

(defn symlink? [qid-type]
  (if (map? qid-type)
    (symlink? (:type qid-type))
    (pos? (bit-and qid-type proto/QTSYMLINK))))

(defn permission?
  "Return true if some stat-like map allows some specific permission for uid,
  otherwise false.
  Target-perm is expect to be an Access-type permission"
  [fd-stat uid target-perm]
  ;; Shield your eyes - we need to bash m as we conditionally check chunks of the perm bits
  (let [mode (:mode fd-stat)
        m (volatile! (bit-and mode 7))]
    (cond
      (= target-perm (bit-and target-perm @m)) true
      (and (= (:uid fd-stat) uid)
           (do (vreset! m (bit-or @m (bit-and (bit-shift-right mode 6) 7)))
               (= target-perm (bit-and target-perm @m)))) true
      (and (= (:gid fd-stat) uid)
           (do (vreset! m (bit-or @m (bit-and (bit-shift-right mode 3) 7)))
               (= target-perm (bit-and target-perm @m)))) true
      :else false)))

(defn apply-permissions [initial-mode additional-modes]
  (apply bit-or initial-mode additional-modes))

(defn open->access
  "Convert an Open-style permission into an Access-style permission"
  [target-perm]
  (let [np (bit-and target-perm 3)
        access-perm (proto/perm-translation np)]
    (if (pos? (bit-and target-perm proto/OTRUNC))
      (bit-or access-perm proto/AWRITE)
      access-perm)))

(defn mode->stat [mode]
  (bit-or (bit-and mode 0777)
          (bit-shift-right (bit-xor (bit-and mode proto/DMDIR)
                                    proto/DMDIR)
                           16)
          (bit-shift-right (bit-and mode proto/DMDIR) 17)
          (bit-shift-right (bit-and mode proto/DMSYMLINK) 10)
          (bit-shift-right (bit-and mode proto/DMSYMLINK) 12)
          (bit-shift-right (bit-and mode proto/DMSETUID) 8)
          (bit-shift-right (bit-and mode proto/DMSETGID) 8)
          (bit-shift-right (bit-and mode proto/DMSTICKY) 7)))

(comment


  (mode->stat 0664)
  )
