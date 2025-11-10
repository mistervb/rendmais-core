package br.com.rendmais.p2p.discovery;

import br.com.rendmais.common.dto.DataCollectionTaskMessage;
import br.com.rendmais.common.dto.PassiveIncomePeerInfo;
import br.com.rendmais.common.dto.PeerAdvertisement;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.registry.PeerRegistry;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PassiveIncomePeerDiscoveryService extends PeerDiscoveryService {
    
    private static final Logger log = LoggerFactory.getLogger(PassiveIncomePeerDiscoveryService.class);
    private static final long RESOURCE_UPDATE_INTERVAL_SECONDS = 300; // 5 minutes
    private static final double MIN_REPUTATION_SCORE = 0.3;
    private static final int MAX_CONCURRENT_TASKS_DEFAULT = 2;
    private static final long MAX_DAILY_DATA_USAGE_DEFAULT = 100; // 100MB
    
    private final Map<String, PassiveIncomePeerInfo> passiveIncomePeers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService resourceScheduler;
    private final Gson gson = new Gson();
    private volatile boolean resourceUpdatesRunning = false;
    
    public PassiveIncomePeerDiscoveryService(PeerRegistry peerRegistry, String selfNodeId, 
                                           PeerAdvertisementSender advertisementSender) {
        super(peerRegistry, selfNodeId, advertisementSender);
        
        this.resourceScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "passive-income-resource-updater");
            t.setDaemon(true);
            return t;
        });
    }
    
    @Override
    public void start() {
        super.start();
        
        // Start resource update scheduling
        if (!resourceUpdatesRunning) {
            resourceUpdatesRunning = true;
            
            // Immediately broadcast initial resources
            broadcastResourceUpdate();
            
            // Schedule periodic resource updates
            resourceScheduler.scheduleAtFixedRate(this::broadcastResourceUpdate, 
                    RESOURCE_UPDATE_INTERVAL_SECONDS, RESOURCE_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
            
            log.info("Passive income peer discovery service started");
        }
    }
    
    @Override
    public void stop() {
        resourceUpdatesRunning = false;
        resourceScheduler.shutdown();
        
        try {
            if (!resourceScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                resourceScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            resourceScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        super.stop();
        log.info("Passive income peer discovery service stopped");
    }
    
    public void updateLocalPeerResources(PassiveIncomePeerInfo localInfo) {
        passiveIncomePeers.put(getSelfNodeId(), localInfo);
        
        // Immediately broadcast updated resources
        broadcastResourceUpdate();
    }
    
    public List<PassiveIncomePeerInfo> findSuitablePeersForTask(DataCollectionTaskMessage task) {
        // Normalize data type: prefer explicit dataType, else map from taskType
        String normalizedDataType = normalizeDataType(task.getDataType(), task.getTaskType());

        return passiveIncomePeers.values().stream()
                .filter(peer -> peer.getNodeId() != null)
                .filter(peer -> !peer.getNodeId().equals(getSelfNodeId()))
                .filter(peer -> peer.getReputationScore() >= MIN_REPUTATION_SCORE)
                .filter(peer -> peer.canAcceptTask(normalizedDataType,
                        task.getMaxDataUsageMB(), 
                        task.getMaxDurationMinutes()))
                .sorted((a, b) -> {
                    // Sort by reputation score (descending), then by current load (ascending)
                    int reputationCompare = Double.compare(b.getReputationScore(), a.getReputationScore());
                    if (reputationCompare != 0) {
                        return reputationCompare;
                    }
                    
                    // Then by current data usage (ascending)
                    return Long.compare(a.getCurrentDataUsageMB(), b.getCurrentDataUsageMB());
                })
                .limit(5) // Return top 5 suitable peers
                .collect(Collectors.toList());
    }

    // Maps common task types to discovery data types when dataType is missing
    private String normalizeDataType(String dataType, String taskType) {
        if (dataType != null && !dataType.isBlank()) {
            return dataType.trim().toLowerCase();
        }
        if (taskType == null || taskType.isBlank()) {
            return null; // No data type information available
        }
        String t = taskType.trim().toLowerCase();
        switch (t) {
            case "price":
            case "price_collection":
            case "prices":
                return "price";
            case "product":
            case "product_info":
                return "product";
            case "market_research":
                return "market_research";
            case "reviews":
                return "reviews";
            default:
                return t; // Fall back to the provided taskType
        }
    }
    
    public PassiveIncomePeerInfo getPassiveIncomePeerInfo(String nodeId) {
        PassiveIncomePeerInfo info = passiveIncomePeers.get(nodeId);
        System.out.println("DEBUG: [DiscoveryService] getPassiveIncomePeerInfo(" + nodeId + ") => " + (info != null ? "present" : "null"));
        if (info != null) {
            System.out.println("DEBUG: [DiscoveryService] Known passive peers: " + passiveIncomePeers.keySet());
        }
        return info;
    }
    
    @Override
    public void handlePeerAdvertisement(SignedMessage advertisementMessage, String sourceNodeId) {
        super.handlePeerAdvertisement(advertisementMessage, sourceNodeId);
        
        // Note: Basic peer advertisements don't contain PassiveIncomePeerInfo
        // Passive income peer information is exchanged through dedicated DATA_COLLECTION_PEER_ADVERTISEMENT messages
        // This method is kept for compatibility with the base peer discovery service
    }
    
    public void handleResourceUpdate(SignedMessage resourceUpdateMessage, String sourceNodeId) {
        try {
            PassiveIncomePeerInfo resourceInfo = gson.fromJson(resourceUpdateMessage.getPayload(), PassiveIncomePeerInfo.class);
            
            if (resourceInfo != null) {
                updatePassiveIncomePeerInfo(resourceInfo);
                System.out.println("DEBUG: [DiscoveryService] handleResourceUpdate from " + sourceNodeId + " for node " + resourceInfo.getNodeId());
                System.out.println("DEBUG: [DiscoveryService] passiveIncomePeers keys after update: " + passiveIncomePeers.keySet());
                log.debug("Updated resource information for peer {}", sourceNodeId);
            }
        } catch (Exception e) {
            log.error("Error processing resource update from {}", sourceNodeId, e);
        }
    }
    
    private void updatePassiveIncomePeerInfo(PassiveIncomePeerInfo peerInfo) {
        if (peerInfo == null || peerInfo.getNodeId() == null) {
            return;
        }
        
        // Validate peer info
        if (peerInfo.getMaxConcurrentTasks() <= 0) {
            peerInfo.setMaxConcurrentTasks(MAX_CONCURRENT_TASKS_DEFAULT);
        }
        
        if (peerInfo.getMaxDailyDataUsageMB() <= 0) {
            peerInfo.setMaxDailyDataUsageMB(MAX_DAILY_DATA_USAGE_DEFAULT);
        }
        
        // Update peer information
        passiveIncomePeers.put(peerInfo.getNodeId(), peerInfo);
        
        log.debug("Updated passive income peer info for {}: reputation={}, tasks={}, data_usage={}MB", 
                peerInfo.getNodeId(), 
                String.format("%.2f", peerInfo.getReputationScore()),
                peerInfo.getCompletedTasksCount(),
                peerInfo.getCurrentDataUsageMB());
    }
    
    private void broadcastResourceUpdate() {
        if (!resourceUpdatesRunning) {
            return;
        }
        
        try {
            PassiveIncomePeerInfo localInfo = passiveIncomePeers.get(getSelfNodeId());
            if (localInfo == null) {
                log.warn("No local passive income info available for broadcasting");
                return;
            }
            
            // Create resource update message
            SignedMessage resourceUpdate = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_RESOURCE_UPDATE)
                    .payload(gson.toJson(localInfo))
                    .signature("") // Will be signed by sender
                    .build();
            
            // Broadcast to all active peers using the base router
            if (getPeerRegistry() != null) {
                Collection<PeerRegistry.PeerEntry> activePeers = getPeerRegistry().listActivePeers();
                for (PeerRegistry.PeerEntry peerEntry : activePeers) {
                    String peerNodeId = peerEntry.getInfo().getNodeId();
                    if (!peerNodeId.equals(getSelfNodeId())) {
                        // Send resource update to each active peer
                        getAdvertisementSender().sendAdvertisement(peerNodeId, resourceUpdate);
                        log.debug("Broadcasting resource update to peer {}", peerNodeId);
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("Error broadcasting resource update", e);
        }
    }
    
    private PassiveIncomePeerInfo createDefaultLocalPeerInfo() {
        return PassiveIncomePeerInfo.passiveBuilder()
                .nodeId(getSelfNodeId())
                .address("0.0.0.0") // Will be updated with actual address
                .port(0) // Will be updated with actual port
                .wifiOnly(true) // Default to WiFi only for passive income
                .maxConcurrentTasks(MAX_CONCURRENT_TASKS_DEFAULT)
                .maxDailyDataUsageMB(MAX_DAILY_DATA_USAGE_DEFAULT)
                .currentDataUsageMB(0)
                .batteryLevel(100)
                .charging(true)
                .supportedDataTypes(Set.of("prices", "product_info", "market_research"))
                .averageTaskCompletionTime(5.0)
                .completedTasksCount(0)
                .acceptsDataCollectionTasks(true)
                .reputationScore(0.5)
                .lastActivityTimestamp(System.currentTimeMillis())
                .userAgent("RendMais-PassiveIncome-Node/1.0")
                .respectsRobotsTxt(true)
                .requestDelayMs(2000)
                .prefersLowBandwidthTasks(true)
                .maxTaskTimeoutMinutes(30)
                .supportedWebsites(Set.of("amazon.com", "ebay.com", "walmart.com"))
                .build();
    }
    
    public Map<String, PassiveIncomePeerInfo> getPassiveIncomePeers() {
        return new HashMap<>(passiveIncomePeers);
    }
    
    public int getSuitablePeerCountForTask(DataCollectionTaskMessage task) {
        return findSuitablePeersForTask(task).size();
    }
}