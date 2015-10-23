(ns cognitect.clj9p.buffer
  (:refer-clojure :exclude [read-string]))

;; Buffers are used to pack and unpack messages.
;; All messages are binary, packed like structs.
;; TODO: Split this up into WriteBuffer, ReadBuffer, ConvertBuffer
(defprotocol Buffer
  (write-string [t s]) ;; Expected to be UTF-8
  (write-byte-array [t ba]) ;; Writing full byte-array data
  (write-bytes [t bb]) ;; Writing a Buffer of the same type
  (write-nio-bytes [t byte-buffer]) ;; Writing full ByteBuffer data
  (write-zero [t n]) ;; Used for padding
  (write-byte [t b]) ;; Write 1
  (write-short [t s]) ;; Write 2
  (write-int [t i]) ;; Write 4
  (write-long [t l]) ;; Write 8
  (writer-index [t])
  (move-writer-index [t n])
  (read-string [t n])
  (read-bytes [t n])
  (read-byte [t]) ;; Read 1
  (read-short [t]) ;; Read 2
  (read-int [t]) ;; Read 4
  (read-long [t]) ;; Read 8
  (readable-bytes [t])
  (reset-read-index [t])
  (as-byte-array [t])
  (as-byte-buffer [t])
  (ensure-little-endian [t])
  (slice [t index length])
  (clear [t]))

(defprotocol Length
  (length [t])) ;; A protocol form of `count` that we can easily extend

