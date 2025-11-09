package br.com.rendmais.p2p.net;

import br.com.rendmais.p2p.discovery.PeerDiscoveryService;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.codec.JsonMessageDecoder;
import br.com.rendmais.p2p.net.codec.JsonMessageEncoder;
import br.com.rendmais.p2p.net.handler.PeerChannelHandler;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
import br.com.rendmais.p2p.registry.PeerRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PServer {

    private static final Logger log = LoggerFactory.getLogger(P2PServer.class);

    private final int port;
    private final MessageRouter router;
    private final PeerRegistry registry;
    private final ConnectionPool connectionPool;
    private final PeerDiscoveryService discoveryService;
    private final HandshakeHandler handshakeHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public P2PServer(int port, MessageRouter router, PeerRegistry registry, 
                     ConnectionPool connectionPool, PeerDiscoveryService discoveryService,
                     HandshakeHandler handshakeHandler) {
        this.port = port;
        this.router = router;
        this.registry = registry;
        this.connectionPool = connectionPool;
        this.discoveryService = discoveryService;
        this.handshakeHandler = handshakeHandler;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // Framing (4-byte length header)
                        p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                        p.addLast(new JsonMessageDecoder());
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new JsonMessageEncoder());
                        
                        PeerChannelHandler handler = new PeerChannelHandler(router, registry, 
                                handshakeHandler, connectionPool, discoveryService);
                        p.addLast(handler);
                    }
                });

        ChannelFuture f = b.bind(port).sync();
        serverChannel = f.channel();
        log.info("P2P Server started on port {}", port);
    }

    public void stop() {
        log.info("Stopping P2P server");
        
        try {
            if (serverChannel != null) serverChannel.close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        }
        
        log.info("P2P server stopped");
    }
    
    public int getPort() {
        return port;
    }
    
    public boolean isRunning() {
        return serverChannel != null && serverChannel.isActive();
    }
}
