package io.siddhi.experiments;

/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

abstract class StreamCallback {
    public abstract void receive(Map<String, Object> events);
}

interface StreamListener {

    String getChannelId();

    void onMessage(byte[] message);
}

class StreamListenerHolder {
    private Map<String, StreamListener> streamListenerMap = new ConcurrentHashMap<String, StreamListener>();

    public StreamListener getStreamListener(String streamId) {
        return streamListenerMap.get(streamId);
    }

    public void putStreamCallback(StreamListener streamListener) {
        if (this.streamListenerMap.containsKey(streamListener.getChannelId())) {
            throw new RuntimeException("TCP source for channelId '" + streamListener.getChannelId()
                    + "' already defined !");
        }
        this.streamListenerMap.put(streamListener.getChannelId(), streamListener);
    }

    public void removeStreamCallback(String streamId) {
        this.streamListenerMap.remove(streamId);
    }

    public int getNoOfRegisteredStreamListeners() {
        return streamListenerMap.size();
    }
}


/**
 * TCP Netty Server.
 */
public class TCPNettyServer {
    private ServerBootstrap bootstrap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private String hostAndPort;
    private ChannelFuture channelFuture;
    private StreamListenerHolder streamInfoHolder = new StreamListenerHolder();

    public synchronized void addStreamListener(StreamListener streamListener) {
        streamInfoHolder.putStreamCallback(streamListener);
    }

    public void start(ServerConfig serverConfig) {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            hostAndPort = serverConfig.getHost() + ":" + serverConfig.getPort();
            bossGroup = new NioEventLoopGroup(serverConfig.getReceiverThreads());
            workerGroup = new NioEventLoopGroup(serverConfig.getWorkerThreads());
            bootstrap = new ServerBootstrap();
            //final SslContext sslCtx;
            //SelfSignedCertificate ssc = new SelfSignedCertificate();
            //sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer() {
                        @Override
                        public void initChannel(Channel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            //p.addLast(sslCtx.newHandler(ch.alloc()));
                            p.addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new ObjectEchoServerHandler(streamInfoHolder));
                        }
                    })
                    .option(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            try {
                // Bind and start to accept incoming connections.
                channelFuture = bootstrap.bind(serverConfig.getHost(), serverConfig.getPort()).sync();
                System.out.println("Tcp Server started in " + hostAndPort + "");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                group.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class ObjectEchoServerHandler extends ChannelInboundHandlerAdapter {
    StreamListenerHolder streamListenerHolder;

    ObjectEchoServerHandler(StreamListenerHolder sl) {
        this.streamListenerHolder = sl;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the client.
        //ctx.write(msg);
        Map<String, Object> mappedBean = (Map<String, Object>) msg;
        StreamListener streamListener = this.streamListenerHolder.getStreamListener((String) mappedBean.get("channelId"));
        streamListener.onMessage((byte[]) mappedBean.get("message"));
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}

class TCPNettyClient {
    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private Channel channel;
    private String sessionId;
    private String hostAndPort;
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public TCPNettyClient(boolean keepAlive, boolean noDelay) {
        this(0, keepAlive, noDelay);
    }

    public TCPNettyClient() {
        this(0, true, true);
    }

    public TCPNettyClient(int numberOfThreads, boolean keepAlive, boolean noDelay) {
        group = new NioEventLoopGroup(numberOfThreads);
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, noDelay)
                .handler(new ChannelInitializer() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        //p.addLast(sslCtx.newHandler(ch.alloc()));
                        p.addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                new ObjectEchoClientHandler());
                    }
                });
    }

    public void connect(String host, int port) throws RuntimeException {
        // Start the connection attempt.
        try {
            hostAndPort = host + ":" + port;
            channel = bootstrap.connect(host, port).sync().channel();
            sessionId = UUID.randomUUID() + "-" + hostAndPort;
        } catch (Throwable e) {
            throw new RuntimeException("Error connecting to '" + hostAndPort + "', " + e.getMessage(), e);
        }
    }

    public ChannelFuture send(final String channelId, final Object message) {
        Map<String, Object> map = new HashMap<>();
        map.put("sessionId", sessionId);
        map.put("channelId", channelId);
        map.put("message", message);
        ChannelFuture cf = channel.writeAndFlush(map);
        return cf;
    }

    public void disconnect() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
                channel.closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            channel.disconnect();
        }
    }

    public void shutdown() {
        disconnect();
        if (group != null) {
            group.shutdownGracefully();
        }
        hostAndPort = null;
        sessionId = null;
    }

}

final class Constant {

    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final int DEFAULT_RECEIVER_THREADS = 10;
    public static final int DEFAULT_WORKER_THREADS = 10;
    public static final int DEFAULT_PORT = 9892;
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final boolean DEFAULT_TCP_NO_DELAY = true;
    public static final boolean DEFAULT_KEEP_ALIVE = true;

    private Constant() {

    }
}

class ServerConfig {
    private int receiverThreads = Constant.DEFAULT_RECEIVER_THREADS;
    private int workerThreads = Constant.DEFAULT_WORKER_THREADS;
    private int port = Constant.DEFAULT_PORT;
    private String host = Constant.DEFAULT_HOST;
    private boolean tcpNoDelay = Constant.DEFAULT_TCP_NO_DELAY;
    private boolean keepAlive = Constant.DEFAULT_KEEP_ALIVE;

    public int getReceiverThreads() {
        return receiverThreads;
    }

    public void setReceiverThreads(int receiverThreads) {
        this.receiverThreads = receiverThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }
}

class ObjectEchoClientHandler extends ChannelInboundHandlerAdapter {

    //private final List<Integer> firstMessage;

    /**
     * Creates a client-side handler.
     */
    public ObjectEchoClientHandler() {
        /*firstMessage = new ArrayList<Integer>(TCPNettyClient.SIZE);
        for (int i = 0; i < TCPNettyClient.SIZE; i ++) {
            firstMessage.add(Integer.valueOf(i));
        }*/
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the first message if this handler is a client-side handler.
        //ctx.writeAndFlush(firstMessage);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the server.
        //ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
