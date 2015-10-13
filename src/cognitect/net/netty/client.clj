(ns cognitect.net.netty.client
  (:require [cognitect.net.netty.util :as util])
  (:import (io.netty.channel Channel
                             ChannelFuture
                             ChannelOption
                             EventLoopGroup)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap Bootstrap)
           (io.netty.buffer PooledByteBufAllocator)))

;; Common Channel Classes
(def local-channel-class (try
                           (and (import '(io.netty.channel.local LocalChannel))
                                io.netty.channel.local.LocalChannel)
                         (catch Throwable t
                           nil)))
(def tcp-channel-class (try
                         (and (import '(io.netty.channel.socket.nio NioSocketChannel))
                              io.netty.channel.socket.nio.NioSocketChannel)
                         (catch Throwable t
                           nil)))
(def sctp-channel-class (try
                         (and (import '(io.netty.channel.sctp.nio NioSctpChannel))
                              io.netty.channel.sctp.nio.NioSctpChannel)
                         (catch Throwable t
                           nil)))

(defn client [base-map channel-class handlers]
  (let [client-group ^EventLoopGroup (NioEventLoopGroup.)
        bstrap ^Bootstrap (Bootstrap.)
        context-atom (atom nil)
        chan-init (util/init-pipeline-handler (into [{:channel-active (fn [ctx]
                                                                        (reset! context-atom ctx))}]
                                                    handlers))
        cleanup-fn (fn [m]
                     (.shutdownGracefully client-group))]
    (assert (some? channel-class) "Missing a valid Channel Class, cannot be nil")
    (doto bstrap
      (.group client-group)
      (.channel channel-class)
      (.option ChannelOption/SO_REUSEADDR (:reuseadder base-map true))
      (.option ChannelOption/TCP_NODELAY (:nodelay base-map true))
      (.option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
      (.handler chan-init))
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
