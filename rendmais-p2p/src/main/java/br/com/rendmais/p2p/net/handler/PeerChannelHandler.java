package br.com.rendmais.p2p.net.handler;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.discovery.PeerDiscoveryService;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.ConnectionPool;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
import br.com.rendmais.p2p.registry.PeerRegistry;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerChannelHandler extends SimpleChannelInboundHandler<SignedMessage> {

    private static final Logger log = LoggerFactory.getLogger(PeerChannelHandler.class);

    private final MessageRouter router;
    private final PeerRegistry registry;
    private final HandshakeHandler handshakeHandler;
    private final ConnectionPool connectionPool;
    private final PeerDiscoveryService discoveryService;
    private volatile String peerNodeId; // Set after successful handshake

    public PeerChannelHandler(MessageRouter router, PeerRegistry registry, 
                            HandshakeHandler handshakeHandler, ConnectionPool connectionPool,
                            PeerDiscoveryService discoveryService) {
        this.router = router;
        this.registry = registry;
        this.handshakeHandler = handshakeHandler;
        this.connectionPool = connectionPool;
        this.discoveryService = discoveryService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel active: {}", ctx.channel().remoteAddress());
        // Nothing automatic here — handshake will be initiated by client connect or server response
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SignedMessage signedMessage) throws Exception {
        log.debug("Received signed message: type={} from {}", signedMessage.getType(), 
                peerNodeId != null ? peerNodeId : "unknown");
        
        // Update connection activity
        if (peerNodeId != null && connectionPool != null) {
            ConnectionPool.ConnectionEntry entry = connectionPool.getConnection(peerNodeId);
            if (entry != null) {
                entry.updateActivity();
            }
        }
        
        if (MessageType.HANDSHAKE.equals(signedMessage.getType())) {
            log.debug("Processing handshake message from {}", ctx.channel().remoteAddress());
            log.debug("Handshake payload: {}", signedMessage.getPayload());
            boolean ok = handshakeHandler.handle(signedMessage);
            if (ok) {
                // Extract peer node ID from handshake
                String realNodeId = extractPeerNodeIdFromHandshake(signedMessage);
                log.debug("Extracted peer node ID: {} from handshake (current peerNodeId: {})", realNodeId, peerNodeId);
                
                // Register connection with the real node ID from handshake
                if (connectionPool != null && realNodeId != null) {
                    // If we had a temporary peerNodeId (from P2PClient), update the connection entry
                    if (peerNodeId != null && !peerNodeId.equals(realNodeId)) {
                        log.info("Updating connection from temporary ID {} to real ID {}", peerNodeId, realNodeId);
                        connectionPool.updateConnectionId(peerNodeId, realNodeId, ctx.channel());
                    } else {
                        log.info("Registering connection for peer: {} (no temporary key to replace)", realNodeId);
                        connectionPool.registerConnection(realNodeId, ctx.channel());
                    }
                    
                    // Update registry with the actual remote address (avoid 0.0.0.0)
                    try {
                        InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
                        if (remote != null) {
                            registry.updatePeerAddress(realNodeId, remote.getHostString(), remote.getPort());
                        }
                    } catch (Exception e) {
                        log.debug("Could not update peer address from channel: {}", e.getMessage());
                    }

                    // Update our peerNodeId to the real one
                    this.peerNodeId = realNodeId;
                } else {
                    log.warn("Failed to register connection: connectionPool={}, realNodeId={}", connectionPool, realNodeId);
                }
                
                // Reply with our handshake (mutual)
                SignedMessage reply = handshakeHandler.buildSignedHandshake();
                ctx.writeAndFlush(reply);
                
                log.info("Handshake successful with peer {}", peerNodeId);
                
                // Trigger immediate discovery advertisement to the newly connected peer
                if (discoveryService != null && peerNodeId != null) {
                    log.debug("Triggering immediate discovery advertisement to peer {}", peerNodeId);
                    discoveryService.onPeerConnected(peerNodeId);
                }
            } else {
                log.warn("Handshake failed verification — closing channel {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        } else if (MessageType.PEER_ADVERTISEMENT.equals(signedMessage.getType())) {
            if (discoveryService != null && peerNodeId != null) {
                discoveryService.handlePeerAdvertisement(signedMessage, peerNodeId);
            }
        } else if (MessageType.PEER_DISCOVERY.equals(signedMessage.getType())) {
            if (discoveryService != null && peerNodeId != null) {
                discoveryService.handlePeerDiscoveryRequest(signedMessage, peerNodeId);
            }
        } else {
            // Route other types via router with peer context
            if (peerNodeId != null) {
                router.route(signedMessage, peerNodeId);
            } else {
                router.route(signedMessage);
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel inactive: {}", ctx.channel().remoteAddress());
        
        if (peerNodeId != null && connectionPool != null) {
            connectionPool.onChannelInactive(ctx.channel());
        }
        
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Channel error {} -> {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
    
    private String extractPeerNodeIdFromHandshake(SignedMessage handshake) {
        try {
            // Parse the handshake payload to extract the peer's node ID
            String payload = handshake.getPayload();
            if (payload != null && !payload.isEmpty()) {
                // Use the same Gson instance from HandshakeHandler to parse PeerInfo
                com.google.gson.Gson gson = new com.google.gson.Gson();
                br.com.rendmais.common.dto.PeerInfo peerInfo = gson.fromJson(payload, br.com.rendmais.common.dto.PeerInfo.class);
                if (peerInfo != null && peerInfo.getNodeId() != null) {
                    log.debug("Successfully extracted peer node ID: {} from handshake payload", peerInfo.getNodeId());
                    return peerInfo.getNodeId();
                }
            }
            log.warn("Could not extract peer node ID from handshake payload: {}", payload);
            return null;
        } catch (Exception e) {
            log.warn("Failed to extract peer node ID from handshake", e);
            return null;
        }
    }
    
    // Method to set peer node ID for outgoing connections
    public void setPeerNodeId(String peerNodeId) {
        this.peerNodeId = peerNodeId;
    }
}
