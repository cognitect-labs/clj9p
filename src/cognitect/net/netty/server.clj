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

(ns cognitect.net.netty.server
  (:require [cognitect.net.netty.util :as util])
  (:import (java.net SocketAddress)
           (io.netty.channel Channel
                             ChannelFuture
                             ChannelOption
                             EventLoopGroup)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap ServerBootstrap
                               ChannelFactory)
           (io.netty.buffer PooledByteBufAllocator)))

;; Common Channel Classes
(def local-channel-class (util/maybe-class 'io.netty.channel.local.LocalServerChannel))
(def tcp-channel-class   (util/maybe-class 'io.netty.channel.socket.nio.NioServerSocketChannel))
(def sctp-channel-class  (util/maybe-class 'io.netty.channel.sctp.nio.NioSctpServerChannel))
(def udt-channel-factory (and (util/maybe-class 'com.barchart.udt.nio.SelectorProviderUDT)
                              (util/maybe-class 'com.barchart.udt.nio.SelectorUDT)
                              (util/maybe-class 'io.netty.channel.udt.nio.NioUdtProvider 'BYTE_ACCEPTOR)))


(defn- configure-netty-bootstrap
  [^EventLoopGroup boss-group ^EventLoopGroup worker-group channel-class backlog reuseaddr chan-init]
  (let [bootstrap (doto (ServerBootstrap.)
                    (.group boss-group worker-group)
                    (.option ChannelOption/SO_BACKLOG backlog)
                    (.option ChannelOption/SO_REUSEADDR reuseaddr)
                    (.childOption ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
                    (.childHandler chan-init))]
    (if (util/channel-factory? channel-class)
      (.channelFactory bootstrap ^ChannelFactory channel-class)
      (.channel bootstrap channel-class))))

(defn- cleanup-handler [& groups]
  (fn [_]
    (doseq [g groups]
      (.shutdownGracefully ^EventLoopGroup g))))

(defn- handle-stop [{:keys [channel] :as server-context}]
  (when channel
    (.close ^Channel channel))
  server-context)

(defn- handle-bind [{:keys [bootstrap port host] :as server-context}]
  (let [fut (if host
              (.bind ^ServerBootstrap bootstrap ^String host (int port))
              (.bind ^ServerBootstrap bootstrap (int port)))]
    (.channel ^ChannelFuture (.sync fut))))

(defn server
  ([channel-class base-map handlers]
  (assert (or (util/channel-class? channel-class)
              (util/channel-factory? channel-class)) "Missing a valid Channel class or ChannelFactory object, cannot be nil")
  (let [cores (:cores base-map (.availableProcessors (Runtime/getRuntime)))
        boss-group   (or (:boss-group base-map)
                         (when (= channel-class udt-channel-factory)
                           (util/event-loop-group 1 "accept" NioUdtProvider/BYTE_PROVIDER))
                         (util/event-loop-group 1))
        worker-group (or (:worker-group base-map)
                         (when (= channel-class udt-channel-factory)
                           (util/event-loop-group (* 2 cores) "connect" NioUdtProvider/BYTE_PROVIDER))
                         (util/event-loop-group))
        ssl-context (:ssl-context base-map)
        ;; TODO: If ssl-context we need to prepend an SSLHandler to `handlers`.  Use SslContextBuilder to build ssl-context
        chan-init    (util/init-pipeline-handler handlers)
        backlog      (int (:backlog base-map 100))
        reuseaddr?   (:reuseaddr base-map true)
        bstrap       (configure-netty-bootstrap boss-group worker-group channel-class backlog reuseaddr? chan-init)]
    (assoc base-map
           :cores cores
           :bootstrap bstrap
           :cleanup   (cleanup-handler boss-group worker-group)
           :stop-fn   handle-stop
           :bind      handle-bind))))

(def start    #(util/start :bind %))
(def stop     util/stop)
(def shutdown util/shutdown)


(comment

  (def echo-serv (server tcp-channel-class
                         {:port 8888 :join? false}
                         [{:channel-read (fn [ctx msg]
                                           (println "Server got: " msg)
                                           (.write ctx msg))
                           :channel-read-complete (fn [ctx] (.flush ctx))}]))
  ;; NOTE: The server joins the main thread // .sync ; this will lock your REPL
  (start echo-serv)

  )
