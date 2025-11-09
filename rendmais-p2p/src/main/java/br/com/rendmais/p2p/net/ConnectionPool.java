package br.com.rendmais.p2p.net;

import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.p2p.registry.PeerRegistry;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {
    
    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);
    private static final long CONNECTION_TIMEOUT_MS = 30000; // 30 seconds
    private static final long RECONNECT_DELAY_MS = 2000; // 2 seconds
    private static final int MAX_RECONNECT_ATTEMPTS = 10;
    
    private final Map<String, ConnectionEntry> connections = new ConcurrentHashMap<>();
    private final PeerRegistry peerRegistry;
    private volatile P2PClient p2pClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Thread connectionMonitor;
    
    public static class ConnectionEntry {
        private final String nodeId;
        private volatile Channel channel;
        private volatile long lastActivity;
        private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
        private volatile boolean shouldReconnect = true;
        
        public ConnectionEntry(String nodeId) {
            this.nodeId = nodeId;
            this.lastActivity = System.currentTimeMillis();
        }
        
        public String getNodeId() {
            return nodeId;
        }
        
        public Channel getChannel() {
            return channel;
        }
        
        public void setChannel(Channel channel) {
            this.channel = channel;
            this.lastActivity = System.currentTimeMillis();
            if (channel != null && channel.isActive()) {
                this.reconnectAttempts.set(0);
            }
        }
        
        public long getLastActivity() {
            return lastActivity;
        }
        
        public void updateActivity() {
            this.lastActivity = System.currentTimeMillis();
        }
        
        public boolean isConnected() {
            return channel != null && channel.isActive();
        }
        
        public int getReconnectAttempts() {
            return reconnectAttempts.get();
        }
        
        public void incrementReconnectAttempts() {
            reconnectAttempts.incrementAndGet();
        }
        
        public void resetReconnectAttempts() {
            reconnectAttempts.set(0);
        }
        
        public boolean shouldReconnect() {
            return shouldReconnect && reconnectAttempts.get() < MAX_RECONNECT_ATTEMPTS;
        }
        
        public void setShouldReconnect(boolean shouldReconnect) {
            this.shouldReconnect = shouldReconnect;
        }
    }
    
    public ConnectionPool(PeerRegistry peerRegistry, P2PClient p2pClient) {
        this.peerRegistry = peerRegistry;
        this.p2pClient = p2pClient;
        this.connectionMonitor = new Thread(this::monitorConnections, "connection-monitor");
        this.connectionMonitor.setDaemon(true);
    }
    
    public void setClient(P2PClient client) {
        this.p2pClient = client;
        log.debug("ConnectionPool client reference set");
    }
    
    public void start() {
        if (running.compareAndSet(false, true)) {
            connectionMonitor.start();
            log.info("Connection pool started");
        }
    }
    
    public void stop() {
        if (running.compareAndSet(true, false)) {
            connectionMonitor.interrupt();
            
            // Close all connections
            connections.values().forEach(entry -> {
                if (entry.getChannel() != null) {
                    entry.getChannel().close();
                }
                entry.setShouldReconnect(false);
            });
            
            connections.clear();
            log.info("Connection pool stopped");
        }
    }
    
    public ConnectionEntry getConnection(String nodeId) {
        return connections.get(nodeId);
    }
    
    public ConnectionEntry createOrGetConnection(String nodeId) {
        return connections.computeIfAbsent(nodeId, ConnectionEntry::new);
    }
    
    public void registerConnection(String nodeId, Channel channel) {
        ConnectionEntry entry = createOrGetConnection(nodeId);
        entry.setChannel(channel);
        
        // Update peer registry connection state
        peerRegistry.updatePeerConnectionState(nodeId, PeerRegistry.PeerEntry.ConnectionState.CONNECTED);
        
        log.info("Registered connection for peer {} (total connections: {})", nodeId, getConnectedPeerCount());
        
        // Log all current connections for debugging
        log.debug("Current connections: {}", connections.keySet());
    }
    
    public void updateConnectionId(String oldNodeId, String newNodeId, Channel channel) {
        // Remove the old connection entry
        ConnectionEntry oldEntry = connections.remove(oldNodeId);
        if (oldEntry != null) {
            // Create new entry with the new node ID but preserve the same channel
            ConnectionEntry newEntry = createOrGetConnection(newNodeId);
            newEntry.setChannel(channel);
            
            // Update peer registry connection states
            peerRegistry.updatePeerConnectionState(oldNodeId, PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
            peerRegistry.updatePeerConnectionState(newNodeId, PeerRegistry.PeerEntry.ConnectionState.CONNECTED);
            
            log.info("Updated connection ID from {} to {} (total connections: {})", oldNodeId, newNodeId, getConnectedPeerCount());
            log.debug("Current connections: {}", connections.keySet());

            // Record the actual remote address for robust reconnection
            try {
                java.net.InetSocketAddress remote = (java.net.InetSocketAddress) channel.remoteAddress();
                if (remote != null) {
                    p2pClient.recordPeerAddress(newNodeId, remote);
                }
            } catch (Exception e) {
                log.debug("Could not record remote address for {}: {}", newNodeId, e.getMessage());
            }
        } else {
            log.warn("No connection found for old node ID {} when updating to {}", oldNodeId, newNodeId);
            // Fallback to regular registration
            registerConnection(newNodeId, channel);
        }
    }
    
    public void unregisterConnection(String nodeId) {
        ConnectionEntry entry = connections.get(nodeId);
        if (entry != null) {
            if (entry.getChannel() != null) {
                entry.getChannel().close();
            }
            entry.setShouldReconnect(false);
            
            // Update peer registry connection state
            peerRegistry.updatePeerConnectionState(nodeId, PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
            
            log.info("Unregistered connection for peer {}", nodeId);
        }
    }
    
    public boolean sendMessage(String nodeId, SignedMessage message) {
        ConnectionEntry entry = getConnection(nodeId);
        if (entry == null || !entry.isConnected()) {
            log.warn("Cannot send message to {}: no active connection", nodeId);
            return false;
        }
        
        try {
            entry.getChannel().writeAndFlush(message);
            entry.updateActivity();
            return true;
        } catch (Exception e) {
            log.error("Failed to send message to {}", nodeId, e);
            return false;
        }
    }
    
    public boolean isConnected(String nodeId) {
        ConnectionEntry entry = getConnection(nodeId);
        return entry != null && entry.isConnected();
    }
    
    public int getConnectedPeerCount() {
        return (int) connections.values().stream()
                .filter(ConnectionEntry::isConnected)
                .count();
    }
    
    public Map<String, ConnectionEntry> getAllConnections() {
        return new ConcurrentHashMap<>(connections);
    }
    
    private void monitorConnections() {
        while (running.get()) {
            try {
                Thread.sleep(1000); // Check every 1 second
                
                // Check for stale connections
                long now = System.currentTimeMillis();
                
                for (ConnectionEntry entry : connections.values()) {
                    if (entry.isConnected()) {
                        // Check for timeout
                        if (now - entry.getLastActivity() > CONNECTION_TIMEOUT_MS) {
                            log.warn("Connection to {} timed out", entry.getNodeId());
                            handleConnectionTimeout(entry);
                        }
                    } else if (entry.shouldReconnect()) {
                        // Attempt reconnection
                        attemptReconnection(entry);
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in connection monitor", e);
            }
        }
    }
    
    private void handleConnectionTimeout(ConnectionEntry entry) {
        if (entry.getChannel() != null) {
            entry.getChannel().close();
        }
        entry.setChannel(null);
        peerRegistry.updatePeerConnectionState(entry.getNodeId(), PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
        
        log.info("Connection to {} timed out and was closed", entry.getNodeId());
    }
    
    private void attemptReconnection(ConnectionEntry entry) {
        String nodeId = entry.getNodeId();
        
        if (entry.getReconnectAttempts() >= MAX_RECONNECT_ATTEMPTS) {
            log.warn("Max reconnection attempts reached for peer {}", nodeId);
            entry.setShouldReconnect(false);
            return;
        }
        
        entry.incrementReconnectAttempts();
        
        try {
            log.info("Attempting reconnection to {} (attempt {})", nodeId, entry.getReconnectAttempts());
            
            // Attempt connection via recorded nodeId address first; fallback to registry
            boolean attempted = p2pClient.reconnectByNodeId(nodeId);
            if (!attempted) {
                PeerRegistry.PeerEntry peerEntry = peerRegistry.getById(nodeId).orElse(null);
                if (peerEntry == null) {
                    log.warn("Cannot reconnect to {}: peer not found in registry", nodeId);
                    return;
                }
                PeerInfo peerInfo = peerEntry.getInfo();
                p2pClient.connect(peerInfo.getAddress(), peerInfo.getPort());
            }
            
            // Wait a bit before next attempt
            Thread.sleep(RECONNECT_DELAY_MS);
            
        } catch (Exception e) {
            log.error("Reconnection attempt {} failed for peer {}", entry.getReconnectAttempts(), nodeId, e);
        }
    }
    
    public void onChannelInactive(Channel channel) {
        // Find the connection entry for this channel
        for (ConnectionEntry entry : connections.values()) {
            if (entry.getChannel() == channel) {
                log.info("Channel for peer {} became inactive", entry.getNodeId());
                entry.setChannel(null);
                peerRegistry.updatePeerConnectionState(entry.getNodeId(), PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
                // Trigger immediate reconnect attempt using known nodeId address
                try {
                    boolean attempted = p2pClient.reconnectByNodeId(entry.getNodeId());
                    if (!attempted) {
                        PeerRegistry.PeerEntry peerEntry = peerRegistry.getById(entry.getNodeId()).orElse(null);
                        if (peerEntry != null) {
                            PeerInfo info = peerEntry.getInfo();
                            p2pClient.connect(info.getAddress(), info.getPort());
                        }
                    }
                } catch (Exception e) {
                    log.debug("Immediate reconnect initiation failed for {}: {}", entry.getNodeId(), e.getMessage());
                }
                break;
            }
        }
    }
}