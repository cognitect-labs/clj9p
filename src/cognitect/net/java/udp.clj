(ns cognitect.net.java.udp
  (:require [clojure.core.async :as async])
  (:import (java.net DatagramPacket DatagramSocket
                     InetAddress)))

(defn socket
  ([]
   (doto (DatagramSocket.)
     (.setReuseAddr true)))
  ([port]
   (doto (DatagramSocket. port)
     (.setReuseAddr true))))

(defn address [^String host]
  (InetAddress/getByName host))

(defn packet
  [^bytes payload ^InetAddress addr port]
  (DatagramPacket. payload (count payload) addr port))

(defn send-udp
  ([^DatagramSocket socket ^DatagramPacket packet]
   (.send socket packet))
  ([^DatagramSocket socket ^InetAddress addr port ^bytes payload]
   (.send socket (packet payload addr port))))

(defn recv-udp
  ([^DatagramSocket socket]
   (let [barr (byte-array 1024)]
     (recv-udp socket (DatagramPacket. barr 1024))))
  ([^DatagramSocket socket ^DatagramPacket packet]
   (.receive socket packet)
   packet))

(defn udp-client
  "Returns a function that can be used to send UDP data to the `host` and `port`"
  [^String host port]
  (let [addr (address host)
        sock (socket)]
    #(send-udp sock addr port %)))

(defn udp-recv-ch
  "Create a channel that is constantly receiving data from a UDP socket.
  If the socket gets `.close`d, the channel will also close.
  If the channel gets closed, the socket will be closed."
  ([port]
   (udp-recv-ch (async/chan 10)))
  ([port ch]
   (let [sock (socket port)]
     (async/thread
       (while (not (.isClosed socket))
         (when-not (async/>! ch (recv-udp socket))
           (.close sock)))
       (async/close! ch))
     {:socket sock
      :chan ch})))

(comment
  (def qgc-port 14550)
  (def recv-port 14551)
  )
