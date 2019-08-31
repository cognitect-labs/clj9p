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

(ns cognitect.net.netty.client
  (:require [cognitect.net.netty.util :as util])
  (:import (io.netty.channel Channel
                             ChannelFuture
                             ChannelOption
                             EventLoopGroup)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap Bootstrap
                               ChannelFactory)
           (io.netty.buffer PooledByteBufAllocator)))

;; Common Channel Classes
(def local-channel-class (util/maybe-class 'io.netty.channel.local.LocalChannel))
(def tcp-channel-class (util/maybe-class 'io.netty.channel.socket.nio.NioSocketChannel))
(def sctp-channel-class (util/maybe-class 'io.netty.channel.sctp.nio.NioSctpChannel))
(def udt-channel-factory (and (util/maybe-class 'com.barchart.udt.nio.SelectorProviderUDT)
                              (util/maybe-class 'com.barchart.udt.nio.SelectorUDT)
                              (util/maybe-class 'io.netty.channel.udt.nio.NioUdtProvider 'BYTE_CONNECTOR)))

(defn client [base-map channel-class handlers]
  (let [cores (:cores base-map (.availableProcessors (Runtime/getRuntime)))
        client-group ^EventLoopGroup (or (:client-group base-map)
                                         (when (= channel-class udt-channel-factory)
                                           (util/event-loop-group (* 2 cores) "connect" NioUdtProvider/BYTE_PROVIDER))
                                       (NioEventLoopGroup.))
        bstrap ^Bootstrap (Bootstrap.)
        context-atom (atom nil)
        chan-init (util/init-pipeline-handler (into [{:channel-active (fn [ctx]
                                                                        (reset! context-atom ctx))}]
                                                    handlers))
        cleanup-fn (fn [m]
                     (.shutdownGracefully client-group))]
    (assert (or (util/channel-class? channel-class)
              (util/channel-factory? channel-class)) "Missing a valid Channel class or ChannelFactory object, cannot be nil")
    (doto bstrap
      (.group client-group)
      (.option ChannelOption/SO_REUSEADDR (:reuseadder base-map true))
      (.option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
      (.handler chan-init))
    (when (= channel-class tcp-channel-class)
      (.option bstrap ChannelOption/TCP_NODELAY (:nodelay base-map true)))
    (if (util/channel-factory? channel-class)
      (.channelFactory bstrap ^ChannelFactory channel-class)
      (.channel bstrap channel-class))
    (merge base-map
           {:bootstrap bstrap
            :remote-context context-atom
            :cleanup cleanup-fn
            :stop-fn (fn [m]
                       (when-let [channel ^Channel (:channel m)]
                         (.close channel)
                         m))
            :connect (fn [m]
                       (let [{:keys [bootstrap port host]
                              :or {bootstrap bstrap
                                   port (:port base-map)}} m
                             ^ChannelFuture fut (if host
                                                  (.sync (.connect bootstrap ^String host (int port)))
                                                  (.sync (.connect bootstrap (int port))))]
                         (.channel fut)))})))

(def start #(util/start :connect %))
(def stop util/stop)
(def shutdown util/shutdown)

(comment
  (def echo-client (client {:port 8888 :host "127.0.0.1" :join? true}
                           tcp-channel-class
                           [{:channel-active (fn [ctx] (.writeAndFlush ctx "hello"))
                             :channel-read (fn [ctx msg]
                                             (println "Client got: " msg)
                                             (.write ctx msg))
                             :channel-read-complete (fn [ctx] (.flush ctx))}]))
  (start echo-client)
  )
