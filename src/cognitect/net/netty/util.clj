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

(ns cognitect.net.netty.util
  (:import java.net.SocketAddress
           (io.netty.channel Channel
                             ChannelHandler
                             ChannelInboundHandler
                             ChannelOutboundHandler
                             ChannelHandlerContext
                             ChannelPromise
                             ChannelFuture)
           (io.netty.bootstrap ChannelFactory)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.util.concurrent DefaultThreadFactory)))

(defn maybe-class
  ([classname]
   (try
     (eval `(import ~classname))
     (catch Throwable t
       nil)))
  ([classname static-field]
   (when-let [c (maybe-class classname)]
     (eval `(. ~c ~static-field)))))

(defn event-loop-group
  ([]         (NioEventLoopGroup.))
  ([nthreads] (NioEventLoopGroup. nthreads))
  ([nthreads group-name provider]
   (let [thread-factory (DefaultThreadFactory. group-name)]
     (NioEventLoopGroup. nthreads thread-factory provider))))

(defn channel-class? [cls]
  (and (class? cls) (some #{Channel} (supers cls))))

(defn channel-factory? [factory]
  (instance? ChannelFactory factory))

;; This is basically like `ChannelHandlerAdapter`, but let's us control it (and use reify instead of proxy)
;; Also recognizes channel initializers
(defn ->handler [m]
  (reify
    ChannelHandler
    ;(isSharable [this] (:shareable? m true))
    (handlerAdded [this ctx])
    (handlerRemoved [this ctx])
    (exceptionCaught [this ctx t]
      (if-let [f (:exception-caught m)]
        (f ctx t)
        (.fireExceptionCaught ^ChannelHandlerContext ctx ^Throwable t)))

    ChannelInboundHandler
    (channelRegistered [this ctx]
      (if-let [f (:channel-registered m)]
        (f ctx)
        (do
          (when-let [initf (:init-channel m)]
            (initf (.channel ctx))
            (.. ctx (pipeline) (remove this)))
          (.fireChannelRegistered ^ChannelHandlerContext ctx))))
    (channelUnregistered [this ctx]
      (.fireChannelUnregistered ^ChannelHandlerContext ctx))
    (channelActive [this ctx]
      (if-let [f (:channel-active m)]
        (f ctx)
        (.fireChannelActive ^ChannelHandlerContext ctx)))
    (channelInactive [this ctx]
      (if-let [f (:channel-inactive m)]
        (f ctx)
        (.fireChannelInactive ^ChannelHandlerContext ctx)))
    (channelRead [this ctx obj]
      (if-let [f (:channel-read m)]
        (f ctx obj)
        (.fireChannelRead ^ChannelHandlerContext ctx ^Object obj)))
    (channelReadComplete [this ctx]
      (if-let [f (:channel-read-complete m)]
        (f ctx)
        (.fireChannelReadComplete ^ChannelHandlerContext ctx)))
    (userEventTriggered [this ctx event]
      (if-let [f (:user-event-triggered m)]
        (f ctx event)
        (.fireUserEventTriggered ^ChannelHandlerContext ctx ^Object event)))
    (channelWritabilityChanged [this ctx]
      (.fireChannelWritabilityChanged ^ChannelHandlerContext ctx))

    ChannelOutboundHandler
    (bind [this ctx addr p]
      (if-let [f (:bind m)]
        (f ctx addr p)
        (.bind ^ChannelHandlerContext ctx ^SocketAddress addr ^ChannelPromise p)))
    (connect [this ctx remote-addr local-addr p]
      (if-let [f (:connect m)]
        (f ctx remote-addr local-addr p)
        (.connect ^ChannelHandlerContext ctx ^SocketAddress remote-addr ^SocketAddress local-addr ^ChannelPromise p)))
    (disconnect [this ctx p]
      (if-let [f (:disconnect m)]
        (f ctx p)
        (.disconnect ^ChannelHandlerContext ctx ^ChannelPromise p)))
    (close [this ctx p]
      (if-let [f (:close m)]
        (f ctx p)
        (.close ^ChannelHandlerContext ctx ^ChannelPromise p)))
    (deregister [this ctx p]
      (if-let [f (:deregister m)]
        (f ctx p)
        (.deregister ^ChannelHandlerContext ctx ^ChannelPromise p)))
    (read [this ctx]
      (if-let [f (:read m)]
        (f ctx)
        (.read ^ChannelHandlerContext ctx)))
    (write [this ctx msg p]
      (if-let [f (:write m)]
        (f ctx msg p)
        (.write ^ChannelHandlerContext ctx ^Object msg ^ChannelPromise p)))
    (flush [this ctx]
      (if-let [f (:flush m)]
        (f ctx)
        (.flush ^ChannelHandlerContext ctx)))))

(defn init-pipeline-handler [handlers]
  (->handler {:init-channel (fn [^Channel chan]
                              (let [pipeline (.pipeline chan)
                                    processed-handlers (mapv (fn [handler]
                                                               (cond
                                                            (class? handler) (eval `(new ~handler))
                                                            (map? handler) (->handler handler)
                                                            :else handler))
                                                             handlers)
                                    handler-array (into-array ChannelHandler processed-handlers)]
                                (.addLast pipeline handler-array)))}))

(defn start [start-key netty-map]
  (when-let [^Channel channel (and (netty-map start-key)
                                   ((netty-map start-key) netty-map))]
    (if (:join? netty-map)
      (try
        (.. channel (closeFuture) (sync))
        (finally
          ((:cleanup netty-map) netty-map)))
      (assoc netty-map
             :channel channel))))

(defn stop [netty-map]
  (some-> (:stop-fn netty-map)
          (apply [netty-map])))

(defn shutdown [netty-map]
  (some-> (stop netty-map)
          :cleanup
          (apply [netty-map])))

