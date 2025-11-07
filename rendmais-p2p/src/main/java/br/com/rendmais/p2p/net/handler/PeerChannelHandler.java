package br.com.rendmais.p2p.net.handler;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
import br.com.rendmais.p2p.registry.PeerRegistry;
import br.com.rendmais.p2p.identity.PeerIdentity;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerChannelHandler extends SimpleChannelInboundHandler<SignedMessage> {

    private static final Logger log = LoggerFactory.getLogger(PeerChannelHandler.class);

    private final MessageRouter router;
    private final PeerRegistry registry;
    private final HandshakeHandler handshakeHandler;

    public PeerChannelHandler(MessageRouter router, PeerRegistry registry, HandshakeHandler handshakeHandler) {
        this.router = router;
        this.registry = registry;
        this.handshakeHandler = handshakeHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel active: {}", ctx.channel().remoteAddress());
        // Nothing automatic here — handshake will be initiated by client connect or server response
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SignedMessage signedMessage) throws Exception {
        log.debug("Received signed message: type={}", signedMessage.getType());
        if (MessageType.HANDSHAKE.equals(signedMessage.getType())) {
            boolean ok = handshakeHandler.handle(signedMessage);
            if (ok) {
                // Reply with our handshake (mutual)
                SignedMessage reply = handshakeHandler.buildSignedHandshake();
                ctx.writeAndFlush(reply);
            } else {
                log.warn("Handshake failed verification — closing channel {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        } else {
            // route other types via router if implemented
            router.route(signedMessage);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Channel error {} -> {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
