package br.com.rendmais.p2p.net;

import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.codec.JsonMessageDecoder;
import br.com.rendmais.p2p.net.codec.JsonMessageEncoder;
import br.com.rendmais.p2p.net.handler.PeerChannelHandler;
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

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public P2PServer(int port, MessageRouter router, PeerRegistry registry) {
        this.port = port;
        this.router = router;
        this.registry = registry;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // Framing (4-byte length header)
                        p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                        p.addLast(new JsonMessageDecoder());
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new JsonMessageEncoder());
                        p.addLast(new PeerChannelHandler(router, registry));
                    }
                });

        ChannelFuture f = b.bind(port).sync();
        serverChannel = f.channel();
        log.info("P2P Server started on port {}", port);
    }

    public void stop() {
        try {
            if (serverChannel != null) serverChannel.close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        }
    }
}
