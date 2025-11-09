package br.com.rendmais.p2p.protocol;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.registry.PeerRegistry;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager {
    
    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);
    private static final long HEARTBEAT_INTERVAL_SECONDS = 30;
    private static final long HEARTBEAT_TIMEOUT_SECONDS = 90;
    
    private final PeerRegistry peerRegistry;
    private final HeartbeatSender heartbeatSender;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Instant> lastHeartbeatReceived = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();
    private volatile boolean running = false;
    
    public interface HeartbeatSender {
        void sendHeartbeat(String targetNodeId, SignedMessage heartbeat);
        void sendHeartbeatResponse(String targetNodeId, SignedMessage heartbeatResponse);
    }
    
    public HeartbeatManager(PeerRegistry peerRegistry, HeartbeatSender heartbeatSender) {
        this.peerRegistry = peerRegistry;
        this.heartbeatSender = heartbeatSender;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "heartbeat-manager");
            t.setDaemon(true);
            return t;
        });
    }
    
    public void start() {
        if (running) {
            log.warn("Heartbeat manager is already running");
            return;
        }
        
        running = true;
        
        // Schedule periodic heartbeat sending
        scheduler.scheduleAtFixedRate(this::sendHeartbeatsToAllPeers, 
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        // Schedule periodic heartbeat timeout checking
        scheduler.scheduleAtFixedRate(this::checkHeartbeatTimeouts, 
                HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        log.info("Heartbeat manager started with interval: {} seconds", HEARTBEAT_INTERVAL_SECONDS);
    }
    
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Heartbeat manager stopped");
    }
    
    public void handleHeartbeat(SignedMessage heartbeat, String sourceNodeId) {
        log.debug("Received heartbeat from: {}", sourceNodeId);
        lastHeartbeatReceived.put(sourceNodeId, Instant.now());
        
        // Send heartbeat response
        Map<String, Object> responseData = new HashMap<>();
        responseData.put("status", "alive");
        responseData.put("timestamp", System.currentTimeMillis());
        
        String payload = gson.toJson(responseData);
        SignedMessage response = SignedMessage.builder()
                .type(MessageType.HEARTBEAT_RESPONSE)
                .payload(payload)
                .signature("") // Will be signed by sender
                .build();
        
        if (heartbeatSender != null) {
            heartbeatSender.sendHeartbeatResponse(sourceNodeId, response);
        }
    }
    
    public void handleHeartbeatResponse(SignedMessage heartbeatResponse, String sourceNodeId) {
        log.debug("Received heartbeat response from: {}", sourceNodeId);
        lastHeartbeatReceived.put(sourceNodeId, Instant.now());
        
        // Update peer state to connected
        peerRegistry.updatePeerConnectionState(sourceNodeId, PeerRegistry.PeerEntry.ConnectionState.CONNECTED);
    }
    
    private void sendHeartbeatsToAllPeers() {
        if (!running) {
            return;
        }
        
        try {
            peerRegistry.listActivePeers().forEach(peerEntry -> {
                String nodeId = peerEntry.getInfo().getNodeId();
                
                Map<String, Object> heartbeatData = new HashMap<>();
                heartbeatData.put("timestamp", System.currentTimeMillis());
                heartbeatData.put("status", "alive");
                
                String payload = gson.toJson(heartbeatData);
                SignedMessage heartbeat = SignedMessage.builder()
                        .type(MessageType.HEARTBEAT)
                        .payload(payload)
                        .signature("") // Will be signed by sender
                        .build();
                
                if (heartbeatSender != null) {
                    heartbeatSender.sendHeartbeat(nodeId, heartbeat);
                }
            });
        } catch (Exception e) {
            log.error("Error sending heartbeats", e);
        }
    }
    
    private void checkHeartbeatTimeouts() {
        if (!running) {
            return;
        }
        
        try {
            Instant cutoff = Instant.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS);
            
            lastHeartbeatReceived.entrySet().removeIf(entry -> {
                String nodeId = entry.getKey();
                Instant lastSeen = entry.getValue();
                
                if (lastSeen.isBefore(cutoff)) {
                    log.warn("Peer {} has timed out (no heartbeat for {} seconds)", nodeId, HEARTBEAT_TIMEOUT_SECONDS);
                    peerRegistry.updatePeerConnectionState(nodeId, PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
                    return true; // Remove from map
                }
                return false;
            });
            
            // Also check peers that haven't sent any heartbeats
            peerRegistry.listActivePeers().forEach(peerEntry -> {
                String nodeId = peerEntry.getInfo().getNodeId();
                if (!lastHeartbeatReceived.containsKey(nodeId)) {
                    log.warn("Peer {} has never sent a heartbeat", nodeId);
                    peerRegistry.updatePeerConnectionState(nodeId, PeerRegistry.PeerEntry.ConnectionState.DISCONNECTED);
                }
            });
            
        } catch (Exception e) {
            log.error("Error checking heartbeat timeouts", e);
        }
    }
    
    public boolean isPeerAlive(String nodeId) {
        Instant lastHeartbeat = lastHeartbeatReceived.get(nodeId);
        if (lastHeartbeat == null) {
            return false;
        }
        
        return lastHeartbeat.isAfter(Instant.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS));
    }
    
    public Instant getLastHeartbeatTime(String nodeId) {
        return lastHeartbeatReceived.get(nodeId);
    }
    
    public int getAlivePeerCount() {
        Instant cutoff = Instant.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS);
        return (int) lastHeartbeatReceived.values().stream()
                .filter(instant -> instant.isAfter(cutoff))
                .count();
    }
}