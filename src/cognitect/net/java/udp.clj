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
