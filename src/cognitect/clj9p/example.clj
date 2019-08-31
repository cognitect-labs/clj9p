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

(ns cognitect.clj9p.example
  (:require [cognitect.clj9p.server :as server]
            [cognitect.clj9p.proto :as proto]))

;; NOTES:
;; ----------
;; You should prefer to send transit-msgpack instead of String data

(def serv (server/server {:app {:greeting "Hello World!"}
                          :ops {:stat server/stat-faker
                                :wstat (server/stub-output-fn {:type :rwstat})
                                :walk server/path-walker
                                :read server/interop-dirreader}
                          :fs {{:type proto/QTDIR :version 0
                                :path "/interjections"} {}
                               {:type proto/QTFILE :version 0
                                :path "/interjections/hello"} {:read (fn [context qid]
                                                                       (let [greeting (get-in context [:server-state :app :greeting])
                                                                             greeting-seeked (subs greeting (get-in context [:input-fcall :offset]))]
                                                                         (server/make-resp context {:type :rread
                                                                                                    :data greeting-seeked})))
                                                               :write (fn [context qid]
                                                                        (let [current-greeting (get-in context [:server-state :app :greeting])
                                                                              {:keys [data offset]} (:input-fcall context)
                                                                              greeting-data (if (string? data) data (String. data "UTF-8"))
                                                                              new-greeting (if (zero? offset)
                                                                                             greeting-data
                                                                                             (str (subs current-greeting 0 offset) greeting-data))]
                                                                          (-> context
                                                                              (assoc :server-state-updater (fn [state]
                                                                                                             (assoc-in state [:app :greeting] new-greeting)))
                                                                              (server/make-resp {:type :rwrite
                                                                                                 :count (count new-greeting)}))))}
                               {:type proto/QTFILE :version 0
                                :path "/interjections/goodbye"} {:read (fn [context qid]
                                                                         (server/make-resp context {:type :rread
                                                                                                    :data "Goodbye!"}))}}}))

(def tcp-serv (server/tcp-server {:flush-every 0
                                  :backlog 100
                                  :reuseaddr true
                                  :port 9090
                                  :host "127.0.0.1"
                                  :join? false}
                                 serv))

;; This 9P implementation supports local chans, JVM IPC, TCP, UDT, and SCTP transports
;; If you want to use UDT, comment out the `def` below, and update the transport-specific calls in the example code
;(def udt-serv (server/udt-server {:flush-every 0
;                                  :backlog 100
;                                  :reuseaddr true
;                                  :port 9091
;                                  :host "127.0.0.1"
;                                  :join? false}
;                                 serv))

(defn start! []
  (require '[cognitect.net.netty.server :as netty])
  (cognitect.net.netty.server/start tcp-serv))

(comment
  ;; Server 1 -- Remote
  (def srv (start!))
  (netty/stop srv)
  ;; Server 2 -- Local-only via channels
  (def srv2 (server/server {:app {:scratchpad {}}
                            :ops {:stat server/stat-faker
                                  :walk server/path-walker
                                  :read server/interop-dirreader}
                            :fs {{:type proto/QTFILE
                                  :path "/cpu"} {:read (fn [context qid]
                                                         (let [client-addr (:cognitect.clj9p.server/remote-addr context)
                                                               repl-result (get-in context [:server-state :app :scratchpad client-addr] "")]
                                                           (server/make-resp context {:type :rread
                                                                                      :data repl-result})))
                                                 :write (fn [context qid]
                                                          (let [data (get-in context [:input-fcall :data] "nil")
                                                                read-input (read-string
                                                                             (if (string? data) data (String. data "UTF-8")) )
                                                                eval-result (str (eval read-input))
                                                                client-addr (:cognitect.clj9p.server/remote-addr context)]
                                                            (-> context
                                                                (assoc :server-state-updater (fn [state]
                                                                                               (assoc-in state [:app :scratchpad client-addr]
                                                                                                         eval-result)))
                                                                (server/make-resp {:type :rwrite
                                                                                   :count (count data)}))))}
                                 {:type proto/QTDIR
                                  :path "/interjections"} {}
                                 {:type proto/QTFILE
                                  :path "/interjections/pardon"} {:read (fn [context qid]
                                                                          (server/make-resp context {:type :rread
                                                                                                     :data "Pardon me"}))}}}))


  (keys @(:state srv))
  (:fs @(:state srv))
  (deref (:clients srv))

  (require '[cognitect.clj9p.client :as clj9p] :reload)
  (def cl (clj9p/client))
  ;(clj9p/mount cl {"/nodes" [(clj9p/tcp-connect {:host "127.0.0.1" :port 9090})]})
  ;(clj9p/mount cl {"/nodes" [[(:server-in srv2) (:server-out srv2)]]})
  (clj9p/mount cl {"/nodes" [(clj9p/tcp-connect {:host "127.0.0.1" :port 9090})
                             [(:server-in srv2) (:server-out srv2)]]})
  ;; To use the channels directly, you need to comment out `tcp-serv` above
  ;(clj9p/mount cl {"/nodes" [[(:server-in serv) (:server-out serv)]]})

  (clj9p/stat cl "/nodes")
  (clj9p/stat cl "/nodes/interjections")
  (clj9p/walk cl "/nodes/interjections/hello")

  (map :name (clj9p/ls cl "/nodes"))
  (map :name (clj9p/ls cl "/nodes/interjections"))
  (clj9p/directory? cl "/nodes/interjections")
  (clj9p/directory? cl "/nodes/interjections/hello")
  (clj9p/file? cl "/nodes/interjections/hello")
  (clj9p/file-type cl "/nodes/interjections/hello")
  (clj9p/mode cl "/nodes/interjections")

  (clj9p/read-str cl "/nodes/interjections/hello")
  (clj9p/write cl "/nodes/interjections/hello" "Hi!\n")
  (clj9p/read cl "/nodes/interjections/NOTHING") ;; Should be an error - no file found
  (clj9p/touch cl "/nodes/interjections/another-greeting") ;; Error, Dir doesn't have perms set
  (clj9p/write cl "/nodes/cpu" "(inc 2)")
  (clj9p/read-str cl "/nodes/cpu")

  (:mounts (deref (:state cl)))
  (clj9p/lsofids cl)
  (clj9p/lsof cl)
  (clj9p/unmount cl "/nodes")
  (clj9p/unmount-all! cl)

  (clj9p/close cl "/nodes/interjections")

  ;; 9pserv.py has /nodes/hello and /nodes/goodbye

  (clj9p/mount cl {"/nodes" [(clj9p/tcp-connect {:host "127.0.0.1" :port 9090})]})
  (clj9p/open cl "/nodes/hello")
  (clj9p/read-str cl "/nodes/hello")
  (clj9p/close cl "/nodes/hello")

  )
