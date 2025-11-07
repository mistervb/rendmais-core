package br.com.rendmais.p2p.net.handler;

import br.com.rendmais.p2p.messaging.Message;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.registry.PeerRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerChannelHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger log = LoggerFactory.getLogger(PeerChannelHandler.class);

    private final MessageRouter router;
    private final PeerRegistry registry;

    public PeerChannelHandler(MessageRouter router, PeerRegistry registry) {
        this.router = router;
        this.registry = registry;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel active: {}", ctx.channel().remoteAddress());
        // Optionally register peer address into registry (best effort)
        // Could send handshake from server-side as well
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        log.debug("Received message from {} type={}", ctx.channel().remoteAddress(), msg.getType());
        router.route(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Channel error {} -> {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
