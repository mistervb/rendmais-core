package br.com.rendmais.p2p.net;

import br.com.rendmais.p2p.messaging.Message;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.codec.JsonMessageDecoder;
import br.com.rendmais.p2p.net.codec.JsonMessageEncoder;
import br.com.rendmais.p2p.net.handler.PeerChannelHandler;
import br.com.rendmais.p2p.registry.PeerRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class P2PClient {

    private static final Logger log = LoggerFactory.getLogger(P2PClient.class);

    private final MessageRouter router;
    private final PeerRegistry registry;
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Bootstrap bootstrap;
    private final InetSocketAddress remoteAddress;

    private volatile Channel channel;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    public P2PClient(String host, int port, MessageRouter router, PeerRegistry registry) {
        this.router = router;
        this.registry = registry;
        this.remoteAddress = new InetSocketAddress(host, port);

        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                        p.addLast(new JsonMessageDecoder());
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new JsonMessageEncoder());
                        p.addLast(new PeerChannelHandler(router, registry));
                    }
                });
    }

    public void connect() {
        doConnect();
    }

    private void doConnect() {
        if (channel != null && channel.isActive()) return;

        bootstrap.connect(remoteAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                channel = future.channel();
                reconnectAttempts.set(0);
                log.info("Connected to peer {}", remoteAddress);
            } else {
                int attempts = reconnectAttempts.incrementAndGet();
                long delay = Math.min(60, 1 << Math.min(attempts, 6)); // exponential backoff up to 60s
                log.warn("Failed to connect to {}. Will retry in {}s", remoteAddress, delay);
                future.channel().eventLoop().schedule(this::doConnect, delay, TimeUnit.SECONDS);
            }
        });
    }

    public void send(Message msg) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(msg);
        } else {
            log.warn("Channel inactive, cannot send. Try connect()");
            doConnect();
        }
    }

    public void shutdown() {
        try {
            if (channel != null) channel.close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            group.shutdownGracefully();
        }
    }
}
