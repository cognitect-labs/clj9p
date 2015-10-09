(ns cognitect.net.netty.server
  (:require [cognitect.net.netty.util :as util])
  (:import (java.net SocketAddress)
           (io.netty.channel Channel
                             ChannelFuture
                             ChannelOption
                             EventLoopGroup)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap ServerBootstrap)
           (io.netty.buffer PooledByteBufAllocator)))

;; Common Channel Classes
(def local-channel-class (util/maybe-class 'io.netty.channel.local.LocalServerChannel))
(def tcp-channel-class   (util/maybe-class 'io.netty.channel.socket.nio.NioServerSocketChannel))
(def sctp-channel-class  (util/maybe-class 'io.netty.channel.sctp.nio.NioSctpServerChannel))

(defn- channel-class? [cls]
  (and (class? cls) (some #{Channel} (supers cls))))

(defn- event-loop-group
  ([]         (NioEventLoopGroup.))
  ([nthreads] (NioEventLoopGroup. nthreads)))

(defn- configure-netty-bootstrap
  [^EventLoopGroup boss-group ^EventLoopGroup worker-group channel-class backlog reuseaddr chan-init]
  (doto (ServerBootstrap.)
    (.group boss-group worker-group)
    (.channel channel-class)
    (.option ChannelOption/SO_BACKLOG backlog)
    (.option ChannelOption/SO_REUSEADDR reuseaddr)
    (.childOption ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
    (.childHandler chan-init)))

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

(defn server [channel-class base-map handlers]
  (assert (channel-class? channel-class) "Missing a valid Channel class, cannot be nil")
  (let [boss-group   (event-loop-group 1)
        worker-group (event-loop-group)
        chan-init    (util/init-pipeline-handler handlers)
        backlog      (int (:backlog base-map 100))
        reuseaddr?   (:reuseaddr base-map true)
        bstrap       (configure-netty-bootstrap boss-group worker-group channel-class backlog reuseaddr? chan-init)]
    (assoc base-map
           :bootstrap bstrap
           :cleanup   (cleanup-handler boss-group worker-group)
           :stop-fn   handle-stop
           :bind      handle-bind)))

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
