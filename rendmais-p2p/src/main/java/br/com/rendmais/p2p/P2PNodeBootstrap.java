package br.com.rendmais.p2p;

import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.discovery.PeerDiscoveryService;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.ConnectionPool;
import br.com.rendmais.p2p.net.P2PClient;
import br.com.rendmais.p2p.net.P2PServer;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
import br.com.rendmais.p2p.protocol.HeartbeatManager;
import br.com.rendmais.p2p.registry.PeerRegistry;
import br.com.rendmais.p2p.identity.PeerIdentity;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Base64;

public class P2PNodeBootstrap {

    private static final Logger log = LoggerFactory.getLogger(P2PNodeBootstrap.class);

    private final PeerIdentity identity;
    @Getter
    private final PeerRegistry registry = new PeerRegistry();
    private final MessageRouter router = new MessageRouter();
    private final ConnectionPool connectionPool;
    private final PeerDiscoveryService discoveryService;
    private final HeartbeatManager heartbeatManager;
    private final P2PServer server;
    private final P2PClient client;
    private final HandshakeHandler handshakeHandler;
    private final int port;

    public P2PNodeBootstrap(int port) {
        this.port = port;
        this.identity = new PeerIdentity("0.0.0.0", port);
        this.connectionPool = new ConnectionPool(registry, null); // Will be set after client creation
        this.discoveryService = new PeerDiscoveryService(registry, identity.getInfo().getNodeId(), 
                new PeerDiscoveryServiceImpl());
        this.heartbeatManager = new HeartbeatManager(registry, new HeartbeatSenderImpl());
        this.handshakeHandler = new HandshakeHandler(registry, identity);
        this.client = new P2PClient(router, registry, handshakeHandler, connectionPool, discoveryService);
        
        // Set the client reference in connection pool
        this.connectionPool.setClient(client);
        
        this.server = new P2PServer(port, router, registry, connectionPool, discoveryService, handshakeHandler);

        // Register message handlers
        registerMessageHandlers();

        // Auto-connect to newly discovered peers
        registry.addDiscoveryListener(new PeerRegistry.PeerDiscoveryListener() {
            @Override
            public void onPeerDiscovered(PeerInfo peerInfo) {
                try {
                    if (!peerInfo.getNodeId().equals(identity.getInfo().getNodeId())) {
                        log.info("Auto-connecting to discovered peer {}", peerInfo.getNodeId());
                        connectTo(peerInfo);
                    }
                } catch (Exception e) {
                    log.warn("Failed to auto-connect to discovered peer {}", peerInfo.getNodeId(), e);
                }
            }

            @Override
            public void onPeerDisconnected(String nodeId) {
                // No-op for now
            }

            @Override
            public void onPeerUpdated(PeerInfo peerInfo) {
                // No-op for now
            }
        });
    }

    private void registerMessageHandlers() {
        // Handshake handler
        router.registerHandler(MessageType.HANDSHAKE, msg -> {
            try {
                PeerInfo info = new com.google.gson.Gson().fromJson(msg.getPayload(), PeerInfo.class);

                // Verify signature
                PublicKey peerPublicKey = KeyUtil.publicKeyFromBase64(info.getPublicKeyBase64());
                boolean verified = KeyUtil.verify(peerPublicKey,
                        msg.getPayload().getBytes(StandardCharsets.UTF_8),
                        Base64.getDecoder().decode(msg.getSignature())
                );

                if (!verified) {
                    log.warn("Handshake signature INVALID for peer {}", info.getNodeId());
                    return;
                }

                registry.addOrUpdatePeer(info, peerPublicKey);
                log.info("Handshake accepted: {}", info.getNodeId());

            } catch (Exception e) {
                log.warn("Invalid handshake payload: {}", e.getMessage());
            }
        });

        // Heartbeat handler
        router.registerHandler(MessageType.HEARTBEAT, msg -> {
            String sourceNodeId = extractNodeIdFromMessage(msg);
            if (sourceNodeId != null) {
                heartbeatManager.handleHeartbeat(msg, sourceNodeId);
            }
        });
        
        // Heartbeat response handler
        router.registerHandler(MessageType.HEARTBEAT_RESPONSE, msg -> {
            String sourceNodeId = extractNodeIdFromMessage(msg);
            if (sourceNodeId != null) {
                heartbeatManager.handleHeartbeatResponse(msg, sourceNodeId);
            }
        });
        
        // Peer advertisement handler
        router.registerHandler(MessageType.PEER_ADVERTISEMENT, msg -> {
            // This will be handled by PeerDiscoveryService through PeerChannelHandler
            log.debug("Peer advertisement received");
        });
        
        // Peer discovery handler
        router.registerHandler(MessageType.PEER_DISCOVERY, msg -> {
            // This will be handled by PeerDiscoveryService through PeerChannelHandler
            log.debug("Peer discovery request received");
        });
    }
    
    private String extractNodeIdFromMessage(SignedMessage msg) {
        try {
            PeerInfo info = new com.google.gson.Gson().fromJson(msg.getPayload(), PeerInfo.class);
            return info.getNodeId();
        } catch (Exception e) {
            log.warn("Failed to extract nodeId from message payload: {}", e.getMessage());
            return null;
        }
    }

    public void start() throws InterruptedException {
        log.info("Starting P2P node bootstrap");
        
        // Start services in order
        connectionPool.start();
        discoveryService.start();
        heartbeatManager.start();
        server.start();
        
        log.info("P2P node bootstrap started successfully");
        log.info("Node ID: {}", identity.getInfo().getNodeId());
        log.info("Listening on port: {}", port);
    }

    public void connectTo(String host, int port) {
        log.info("Connecting to peer {}:{}", host, port);
        client.connect(host, port);
    }
    
    public void connectTo(PeerInfo peerInfo) {
        log.info("Connecting to peer {} at {}:{}", peerInfo.getNodeId(), peerInfo.getAddress(), peerInfo.getPort());
        client.connect(peerInfo);
    }
    
    public void broadcastMessage(SignedMessage message) {
        client.broadcast(message);
    }
    
    public void sendMessage(String peerNodeId, SignedMessage message) {
        client.send(peerNodeId, message);
    }
    
    public boolean isConnectedTo(String peerNodeId) {
        return client.isConnected(peerNodeId);
    }
    
    public int getConnectedPeerCount() {
        return client.getConnectedPeerCount();
    }
    
    public String getLocalNodeId() {
        return identity.getInfo().getNodeId();
    }

    public void stop() {
        log.info("Stopping P2P node bootstrap");
        
        // Stop services in reverse order
        server.stop();
        heartbeatManager.stop();
        discoveryService.stop();
        connectionPool.stop();
        client.shutdown();
        
        log.info("P2P node bootstrap stopped");
    }
    
    // Inner class to implement PeerDiscoveryService.PeerAdvertisementSender
    private class PeerDiscoveryServiceImpl implements PeerDiscoveryService.PeerAdvertisementSender {
        @Override
        public void sendAdvertisement(String targetNodeId, SignedMessage advertisement) {
            sendMessage(targetNodeId, advertisement);
        }

        @Override
        public void sendDiscoveryRequest(String targetNodeId, SignedMessage discoveryRequest) {
            sendMessage(targetNodeId, discoveryRequest);
        }
    }
    
    // Inner class to implement HeartbeatManager.HeartbeatSender
    private class HeartbeatSenderImpl implements HeartbeatManager.HeartbeatSender {
        @Override
        public void sendHeartbeat(String targetNodeId, SignedMessage heartbeat) {
            sendMessage(targetNodeId, heartbeat);
        }

        @Override
        public void sendHeartbeatResponse(String targetNodeId, SignedMessage heartbeatResponse) {
            sendMessage(targetNodeId, heartbeatResponse);
        }
    }
}
