(ns cognitect.net.netty.server
  (:require [cognitect.net.netty.util :as util])
  (:import (java.net SocketAddress)
           (io.netty.channel Channel
                             ChannelFuture
                             ChannelOption)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap ServerBootstrap)
           (io.netty.buffer PooledByteBufAllocator)))

;; Common Channel Classes
(def local-channel-class (try
                           (and (import '(io.netty.channel.local LocalServerChannel))
                                io.netty.channel.local.LocalServerChannel)
                         (catch Throwable t
                           nil)))
(def tcp-channel-class (try
                         (and (import '(io.netty.channel.socket.nio NioServerSocketChannel))
                              io.netty.channel.socket.nio.NioServerSocketChannel)
                         (catch Throwable t
                           nil)))
(def sctp-channel-class (try
                         (and (import '(io.netty.channel.sctp.nio NioSctpServerChannel))
                              io.netty.channel.sctp.nio.NioSctpServerChannel)
                         (catch Throwable t
                           nil)))

(defn server [base-map channel-class handlers]
  (let [boss-group ^EventLoopGroup (NioEventLoopGroup. 1)
        worker-group ^EventLoopGroup (NioEventLoopGroup.)
        bstrap ^ServerBootstrap (ServerBootstrap.)
        chan-init (util/init-pipeline-handler handlers)
        cleanup-fn (fn [m]
                     (.shutdownGracefully boss-group)
                     (.shutdownGracefully worker-group))]
    (assert (some? channel-class) "Missing a valid Channel Class, cannot be nil")
    (doto bstrap
      (.group boss-group worker-group)
      (.channel channel-class)
      (.option ChannelOption/SO_BACKLOG (int (:backlog base-map 100)))
      (.option ChannelOption/SO_REUSEADDR (:reuseadder base-map true))
      (.childOption ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
      (.childHandler chan-init))
    (merge base-map
           {:bootstrap bstrap
            :cleanup cleanup-fn
            :stop-fn (fn [m]
                       (when-let [channel ^Channel (:channel m)]
                         (.close channel)
                         m))
            :bind (fn [m]
                    (let [{:keys [bootstrap port host]
                           :or {bootstrap bstrap
                                port (:port base-map)}} m
                          ^ChannelFuture fut (if host
                                               (.sync (.bind bootstrap ^String host (int port)))
                                               (.sync (.bind bootstrap (int port))))]
                      (.channel fut)))})))

(def start #(util/start :bind %))
(def stop util/stop)
(def shutdown util/shutdown)


(comment

  (def echo-serv (server {:port 8888 :join? false}
                         tcp-channel-class
                         [{:channel-read (fn [ctx msg]
                                           (println "Server got: " msg)
                                           (.write ctx msg))
                           :channel-read-complete (fn [ctx] (.flush ctx))}]))
  ;; NOTE: The server joins the main thread // .sync ; this will lock your REPL
  (start echo-serv)

  )

