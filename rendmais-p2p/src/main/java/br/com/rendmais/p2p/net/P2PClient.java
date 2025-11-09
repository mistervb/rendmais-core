package br.com.rendmais.p2p.net;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.p2p.discovery.PeerDiscoveryService;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.p2p.net.codec.JsonMessageDecoder;
import br.com.rendmais.p2p.net.codec.JsonMessageEncoder;
import br.com.rendmais.p2p.net.handler.PeerChannelHandler;
import br.com.rendmais.p2p.protocol.HandshakeHandler;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class P2PClient {

    private static final Logger log = LoggerFactory.getLogger(P2PClient.class);

    private final HandshakeHandler handshakeHandler;
    private final MessageRouter router;
    private final PeerRegistry registry;
    private final ConnectionPool connectionPool;
    private final PeerDiscoveryService discoveryService;
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Map<String, Bootstrap> bootstraps = new ConcurrentHashMap<>();
    private final Map<String, InetSocketAddress> peerAddresses = new ConcurrentHashMap<>();
    private final Map<String, InetSocketAddress> nodeIdAddresses = new ConcurrentHashMap<>();

    public P2PClient(MessageRouter router, PeerRegistry registry, HandshakeHandler handshakeHandler,
                    ConnectionPool connectionPool, PeerDiscoveryService discoveryService) {
        this.router = router;
        this.registry = registry;
        this.handshakeHandler = handshakeHandler;
        this.connectionPool = connectionPool;
        this.discoveryService = discoveryService;
    }

    public void connect(String host, int port) {
        String peerKey = host + ":" + port;
        InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
        peerAddresses.put(peerKey, remoteAddress);
        
        Bootstrap bootstrap = createBootstrap(peerKey);
        bootstraps.put(peerKey, bootstrap);
        
        doConnect(peerKey, remoteAddress);
    }
    
    public void connect(PeerInfo peerInfo) {
        // Record nodeId -> address mapping if available
        if (peerInfo.getNodeId() != null) {
            nodeIdAddresses.put(peerInfo.getNodeId(), new InetSocketAddress(peerInfo.getAddress(), peerInfo.getPort()));
        }
        connect(peerInfo.getAddress(), peerInfo.getPort());
    }

    private Bootstrap createBootstrap(String peerKey) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                        p.addLast(new JsonMessageDecoder());
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new JsonMessageEncoder());
                        
                        PeerChannelHandler handler = new PeerChannelHandler(router, registry, 
                                handshakeHandler, connectionPool, discoveryService);
                        // Set the peer node ID for outgoing connections
                        handler.setPeerNodeId(peerKey);
                        p.addLast(handler);
                    }
                });
        return bootstrap;
    }

    private void doConnect(String peerKey, InetSocketAddress remoteAddress) {
        ConnectionPool.ConnectionEntry existingEntry = connectionPool.getConnection(peerKey);
        if (existingEntry != null && existingEntry.isConnected()) {
            log.debug("Already connected to peer {}", peerKey);
            return;
        }

        Bootstrap bootstrap = bootstraps.get(peerKey);
        if (bootstrap == null) {
            log.error("No bootstrap found for peer {}", peerKey);
            return;
        }

        bootstrap.connect(remoteAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel channel = future.channel();
                
                // Create or update connection entry (using peerKey temporarily)
                ConnectionPool.ConnectionEntry entry = connectionPool.createOrGetConnection(peerKey);
                entry.setChannel(channel);
                
                log.info("Connected to peer {} at {}", peerKey, remoteAddress);
                
                // Send handshake - this will allow the server to identify itself
                SignedMessage myHandshake = handshakeHandler.buildSignedHandshake();
                log.debug("Sending handshake to peer {}: {}", peerKey, myHandshake.getType());
                channel.writeAndFlush(myHandshake);
            } else {
                log.warn("Failed to connect to peer {} at {}", peerKey, remoteAddress);
                
                // Schedule reconnection attempt
                ConnectionPool.ConnectionEntry entry = connectionPool.getConnection(peerKey);
                if (entry != null && entry.shouldReconnect()) {
                    future.channel().eventLoop().schedule(() -> doConnect(peerKey, remoteAddress), 
                            5000, TimeUnit.MILLISECONDS);
                }
            }
        });
    }

    public void send(String peerNodeId, SignedMessage msg) {
        ConnectionPool.ConnectionEntry entry = connectionPool.getConnection(peerNodeId);
        if (entry != null && entry.isConnected()) {
            entry.getChannel().writeAndFlush(msg);
            entry.updateActivity();
        } else {
            log.warn("No active connection to peer {} for sending message", peerNodeId);
            
            // Attempt to reconnect
            InetSocketAddress address = nodeIdAddresses.get(peerNodeId);
            if (address == null) {
                address = peerAddresses.get(peerNodeId);
            }
            if (address != null) {
                doConnect(peerNodeId, address);
            }
        }
    }

    public void broadcast(SignedMessage msg) {
        connectionPool.getAllConnections().values().stream()
                .filter(ConnectionPool.ConnectionEntry::isConnected)
                .forEach(entry -> {
                    try {
                        entry.getChannel().writeAndFlush(msg);
                        entry.updateActivity();
                    } catch (Exception e) {
                        log.error("Failed to broadcast message to peer {}", entry.getNodeId(), e);
                    }
                });
    }

    public void shutdown() {
        log.info("Shutting down P2P client");
        
        // Close all connections
        connectionPool.getAllConnections().values().forEach(entry -> {
            if (entry.getChannel() != null) {
                entry.getChannel().close();
            }
        });
        
        bootstraps.clear();
        peerAddresses.clear();
        
        group.shutdownGracefully();
        log.info("P2P client shutdown complete");
    }
    
    public boolean isConnected(String peerNodeId) {
        return connectionPool.isConnected(peerNodeId);
    }
    
    public int getConnectedPeerCount() {
        return connectionPool.getConnectedPeerCount();
    }
    
    public PeerDiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    // Record address mapped by nodeId after successful handshake (via ConnectionPool)
    public void recordPeerAddress(String nodeId, InetSocketAddress address) {
        if (nodeId != null && address != null) {
            nodeIdAddresses.put(nodeId, address);
            // Also record in peerAddresses keyed by nodeId for broader fallback
            peerAddresses.put(nodeId, address);
            log.debug("Recorded address for node {} -> {}", nodeId, address);
        }
    }

    // Try reconnect using nodeId mapping; returns true if an attempt was made
    public boolean reconnectByNodeId(String nodeId) {
        InetSocketAddress address = nodeIdAddresses.get(nodeId);
        if (address == null) {
            address = peerAddresses.get(nodeId);
            if (address == null) {
                return false;
            }
        }
        Bootstrap bootstrap = bootstraps.get(nodeId);
        if (bootstrap == null) {
            bootstrap = createBootstrap(nodeId);
            bootstraps.put(nodeId, bootstrap);
        }
        doConnect(nodeId, address);
        return true;
    }
}
