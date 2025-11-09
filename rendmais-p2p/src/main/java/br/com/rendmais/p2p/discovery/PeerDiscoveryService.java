package br.com.rendmais.p2p.discovery;

import br.com.rendmais.common.dto.PeerAdvertisement;
import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.registry.PeerRegistry;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PeerDiscoveryService {
    
    private static final Logger log = LoggerFactory.getLogger(PeerDiscoveryService.class);
    private static final long DISCOVERY_INTERVAL_SECONDS = 60;
    private static final long INITIAL_DISCOVERY_DELAY_SECONDS = 10;
    private static final int MAX_ADVERTISEMENT_PEERS = 20;
    
    private final PeerRegistry peerRegistry;
    private final String selfNodeId;
    private final PeerAdvertisementSender advertisementSender;
    private final ScheduledExecutorService scheduler;
    private final Gson gson = new Gson();
    private final Set<String> processedAdvertisements = ConcurrentHashMap.newKeySet();
    private volatile boolean running = false;
    
    public interface PeerAdvertisementSender {
        void sendAdvertisement(String targetNodeId, SignedMessage advertisement);
        void sendDiscoveryRequest(String targetNodeId, SignedMessage discoveryRequest);
    }
    
    public PeerDiscoveryService(PeerRegistry peerRegistry, String selfNodeId, 
                               PeerAdvertisementSender advertisementSender) {
        this.peerRegistry = peerRegistry;
        this.selfNodeId = selfNodeId;
        this.advertisementSender = advertisementSender;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "peer-discovery");
            t.setDaemon(true);
            return t;
        });
    }
    
    public void start() {
        if (running) {
            log.warn("Peer discovery service is already running");
            return;
        }
        
        running = true;
        
        // Schedule periodic peer advertisements
        scheduler.scheduleAtFixedRate(this::sendPeerAdvertisements, 
                INITIAL_DISCOVERY_DELAY_SECONDS, DISCOVERY_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        // Schedule periodic cleanup of processed advertisements
        scheduler.scheduleAtFixedRate(this::cleanupProcessedAdvertisements, 
                1, 1, TimeUnit.HOURS);
        
        log.info("Peer discovery service started");
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
        log.info("Peer discovery service stopped");
    }
    
    public void handlePeerAdvertisement(SignedMessage advertisementMessage, String sourceNodeId) {
        try {
            PeerAdvertisement advertisement = gson.fromJson(advertisementMessage.getPayload(), PeerAdvertisement.class);
            
            if (advertisement == null) {
                log.warn("Received null peer advertisement from {}", sourceNodeId);
                return;
            }
            
            // Check if we've already processed this advertisement
            String advertisementId = generateAdvertisementId(advertisement);
            if (processedAdvertisements.contains(advertisementId)) {
                log.debug("Skipping already processed advertisement from {}", sourceNodeId);
                return;
            }
            
            // Validate TTL
            if (isAdvertisementExpired(advertisement)) {
                log.debug("Skipping expired advertisement from {}", sourceNodeId);
                return;
            }
            
            log.info("Processing peer advertisement from {} containing {} peers", 
                    sourceNodeId, advertisement.getKnownPeers().size());
            
            // Process known peers
            for (PeerInfo peerInfo : advertisement.getKnownPeers()) {
                if (peerInfo.getNodeId().equals(selfNodeId)) {
                    continue; // Skip self
                }
                
                // Add to registry (public key will be obtained during handshake)
                log.debug("Discovered peer {} from advertisement", peerInfo.getNodeId());
                try {
                    peerRegistry.addOrUpdatePeer(peerInfo, null);
                } catch (Exception e) {
                    log.warn("Failed to add discovered peer {} to registry", peerInfo.getNodeId(), e);
                }
            }
            
            // Mark as processed
            processedAdvertisements.add(advertisementId);
            
        } catch (Exception e) {
            log.error("Error processing peer advertisement from {}", sourceNodeId, e);
        }
    }
    
    public void handlePeerDiscoveryRequest(SignedMessage discoveryRequest, String sourceNodeId) {
        log.debug("Received peer discovery request from {}", sourceNodeId);
        
        // Send immediate advertisement in response
        sendPeerAdvertisementTo(sourceNodeId);
    }
    
    public void onPeerConnected(String peerNodeId) {
        log.info("Peer {} connected, sending immediate advertisement", peerNodeId);
        // Send immediate advertisement when a new peer connects
        sendPeerAdvertisementTo(peerNodeId);
    }
    
    private void sendPeerAdvertisements() {
        if (!running) {
            return;
        }
        
        try {
            // Send advertisements to a subset of connected peers
            List<PeerRegistry.PeerEntry> activePeers = new ArrayList<>(peerRegistry.listActivePeers());
            
            if (activePeers.isEmpty()) {
                log.debug("No active peers to send advertisements to");
                return;
            }
            
            // Send to a random subset of peers to avoid flooding
            Collections.shuffle(activePeers);
            int peersToAdvertiseTo = Math.min(MAX_ADVERTISEMENT_PEERS, activePeers.size());
            
            for (int i = 0; i < peersToAdvertiseTo; i++) {
                PeerRegistry.PeerEntry peer = activePeers.get(i);
                sendPeerAdvertisementTo(peer.getInfo().getNodeId());
            }
            
            log.debug("Sent peer advertisements to {} peers", peersToAdvertiseTo);
            
        } catch (Exception e) {
            log.error("Error sending peer advertisements", e);
        }
    }
    
    private void sendPeerAdvertisementTo(String targetNodeId) {
        try {
            PeerAdvertisement advertisement = peerRegistry.createPeerAdvertisement(selfNodeId);
            String payload = gson.toJson(advertisement);
            
            SignedMessage advertisementMessage = SignedMessage.builder()
                    .type(MessageType.PEER_ADVERTISEMENT)
                    .payload(payload)
                    .signature("") // Will be signed by sender
                    .build();
            
            if (advertisementSender != null) {
                advertisementSender.sendAdvertisement(targetNodeId, advertisementMessage);
            }
            
        } catch (Exception e) {
            log.error("Error sending peer advertisement to {}", targetNodeId, e);
        }
    }
    
    private String generateAdvertisementId(PeerAdvertisement advertisement) {
        return advertisement.getAdvertiserNodeId() + "_" + advertisement.getTimestamp();
    }
    
    private boolean isAdvertisementExpired(PeerAdvertisement advertisement) {
        long ageSeconds = (System.currentTimeMillis() - advertisement.getTimestamp()) / 1000;
        return ageSeconds > advertisement.getTtl();
    }
    
    private void cleanupProcessedAdvertisements() {
        // Simple cleanup - in a real implementation, we might want more sophisticated logic
        if (processedAdvertisements.size() > 10000) {
            processedAdvertisements.clear();
            log.debug("Cleared processed advertisements cache");
        }
    }
    
    public Set<String> getDiscoveredPeers() {
        return peerRegistry.listPeers().stream()
                .map(entry -> entry.getInfo().getNodeId())
                .collect(Collectors.toSet());
    }
    
    public int getDiscoveredPeerCount() {
        return peerRegistry.listPeers().size();
    }
}