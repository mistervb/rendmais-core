package br.com.rendmais.p2p;

import br.com.rendmais.common.crypto.KeyUtil;
import br.com.rendmais.common.dto.DataCollectionTaskMessage;
import br.com.rendmais.common.dto.PassiveIncomePeerInfo;
import br.com.rendmais.common.dto.PeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.discovery.PassiveIncomePeerDiscoveryService;
import br.com.rendmais.p2p.messaging.DataCollectionMessageRouter;
import br.com.rendmais.p2p.net.PassiveIncomeBandwidthManager;
import br.com.rendmais.p2p.registry.PeerRegistry;
import com.google.gson.Gson;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PassiveIncomeP2PNodeBootstrap extends P2PNodeBootstrap {
    
    private static final Logger log = LoggerFactory.getLogger(PassiveIncomeP2PNodeBootstrap.class);
    
    // Passive income components
    private final PassiveIncomePeerDiscoveryService passiveIncomeDiscoveryService;
    private final DataCollectionMessageRouter dataCollectionRouter;
    private final PassiveIncomeBandwidthManager bandwidthManager;
    
    // Local resource tracking
    private final PassiveIncomePeerInfo localPeerInfo;
    private final AtomicInteger currentTaskCount = new AtomicInteger(0);
    private final AtomicInteger totalTasksCompleted = new AtomicInteger(0);
    private final ConcurrentHashMap<String, DataCollectionTaskMessage> localTasks = new ConcurrentHashMap<>();
    
    private final Gson gson = new Gson();
    
    // Configuration
    private final boolean wifiOnly;
    private final long maxDailyDataUsageMB;
    private final int maxConcurrentTasks;
    
    public PassiveIncomeP2PNodeBootstrap(int port, boolean wifiOnly, long maxDailyDataUsageMB, int maxConcurrentTasks) {
        super(port);
        
        this.wifiOnly = wifiOnly;
        this.maxDailyDataUsageMB = maxDailyDataUsageMB;
        this.maxConcurrentTasks = maxConcurrentTasks;
        
        // Initialize bandwidth manager
        this.bandwidthManager = new PassiveIncomeBandwidthManager(
            maxDailyDataUsageMB, // daily limit
            maxDailyDataUsageMB / 4, // hourly limit (25% of daily)
            10, // task limit (10MB per task)
            2000, // 2 second delay between requests
            maxConcurrentTasks // max concurrent requests
        );
        
        // Set WiFi status
        this.bandwidthManager.setWifiOnly(wifiOnly);
        this.bandwidthManager.setCurrentlyOnWifi(true); // Assume WiFi for now
        
        // Create enhanced peer info with passive income capabilities
        // Use the local node ID from the parent class
        String localNodeId = super.getLocalNodeId();
        this.localPeerInfo = PassiveIncomePeerInfo.passiveBuilder()
            .nodeId(localNodeId)
            .address("0.0.0.0") // Will be updated when we start
            .port(0) // Will be updated when we start
            .wifiOnly(wifiOnly)
            .maxConcurrentTasks(maxConcurrentTasks)
            .maxDailyDataUsageMB(maxDailyDataUsageMB)
            .currentDataUsageMB(0)
            .batteryLevel(100)
            .charging(true)
            .supportedDataTypes(Set.of("price", "product", "market_research"))
            .averageTaskCompletionTime(0.0)
            .completedTasksCount(0)
            .acceptsDataCollectionTasks(true)
            .reputationScore(1.0)
            .lastActivityTimestamp(System.currentTimeMillis())
            .userAgent("PassiveIncomeP2P/1.0")
            .respectsRobotsTxt(true)
            .requestDelayMs(1000)
            .prefersLowBandwidthTasks(false)
            .maxTaskTimeoutMinutes(30)
            .supportedWebsites(Set.of())
            .build();
        
        // Replace discovery service with passive income version
        this.passiveIncomeDiscoveryService = new PassiveIncomePeerDiscoveryService(
            super.getRegistry(),
            localPeerInfo.getNodeId(),
            new PassiveIncomePeerAdvertisementSenderImpl()
        );
        
        // Create data collection message router
        this.dataCollectionRouter = new DataCollectionMessageRouter(
            super.getRouter(),
            passiveIncomeDiscoveryService
        );
        
        // Register passive income message handlers
        registerPassiveIncomeMessageHandlers();
        
        // When peers become connected/updated, actively exchange passive income info
        super.getRegistry().addDiscoveryListener(new PeerRegistry.PeerDiscoveryListener() {
            @Override
            public void onPeerDiscovered(PeerInfo peerInfo) {
                // No-op: wait for connection to complete
            }

            @Override
            public void onPeerDisconnected(String nodeId) {
                // No-op for now
            }

            @Override
            public void onPeerUpdated(PeerInfo peerInfo) {
                try {
                    String targetNodeId = peerInfo.getNodeId();
                    System.out.println("DEBUG: [Bootstrap] Peer updated/connected: " + targetNodeId + ", exchanging passive income info");

                    // Send our passive income advertisement directly to the newly connected peer
                    SignedMessage advertisement = SignedMessage.builder()
                            .type(MessageType.DATA_COLLECTION_PEER_ADVERTISEMENT)
                            .payload(gson.toJson(localPeerInfo))
                            .signature("") // Will be signed
                            .build();
                    sendMessage(targetNodeId, advertisement);

                    // Also request their passive income info explicitly
                    SignedMessage discoveryRequest = SignedMessage.builder()
                            .type(MessageType.DATA_COLLECTION_PEER_DISCOVERY)
                            .payload(gson.toJson(localPeerInfo))
                            .signature("") // Will be signed
                            .build();
                    sendMessage(targetNodeId, discoveryRequest);

                } catch (Exception e) {
                    log.warn("Failed to exchange passive income info with peer {}", peerInfo.getNodeId(), e);
                }
            }
        });

        log.info("Initialized PassiveIncomeP2PNodeBootstrap with WiFi-only: {}, daily limit: {}MB, max tasks: {}",
                wifiOnly, maxDailyDataUsageMB, maxConcurrentTasks);
    }
    
    private void registerPassiveIncomeMessageHandlers() {
        // Data collection task request handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_TASK_REQUEST, message -> {
            handleDataCollectionTaskRequest(message);
        });
        
        // Data collection task response handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_TASK_RESPONSE, message -> {
            handleDataCollectionTaskResponse(message);
        });
        
        // Data collection task result handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_TASK_RESULT, message -> {
            handleDataCollectionTaskResult(message);
        });
        
        // Data collection task progress handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_TASK_PROGRESS, message -> {
            handleDataCollectionTaskProgress(message);
        });
        
        // Data collection peer advertisement handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_PEER_ADVERTISEMENT, message -> {
            handlePassiveIncomePeerAdvertisement(message);
        });
        
        // Data collection peer discovery handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_PEER_DISCOVERY, message -> {
            handlePassiveIncomePeerDiscovery(message);
        });
        
        // Data collection resource update handler
        super.getRouter().registerHandler(MessageType.DATA_COLLECTION_RESOURCE_UPDATE, message -> {
            handlePassiveIncomeResourceUpdate(message);
        });
    }
    
    private void handleDataCollectionTaskRequest(SignedMessage message) {
        try {
            DataCollectionTaskMessage task = gson.fromJson(message.getPayload(), DataCollectionTaskMessage.class);
            
            if (task == null || !task.isValidForPassiveIncome()) {
                log.warn("Received invalid data collection task request");
                return;
            }
            
            String sourceNodeId = extractNodeIdFromMessage(message);
            log.info("Received data collection task request: {} from {} (type: {})", 
                    task.getTaskId(), sourceNodeId, task.getTaskType());
            
            // Check if we can accept this task
            if (!canAcceptLocalTask(task)) {
                log.warn("Cannot accept data collection task {} due to resource constraints", task.getTaskId());
                sendTaskResponse(sourceNodeId, task.getTaskId(), false, "Resource constraints prevent task acceptance");
                return;
            }
            
            // Check bandwidth
            if (!bandwidthManager.canMakeRequest(task.getTargetUrl(), task.getMaxDataUsageMB())) {
                log.warn("Cannot accept data collection task {} due to bandwidth constraints", task.getTaskId());
                sendTaskResponse(sourceNodeId, task.getTaskId(), false, "Bandwidth constraints prevent task acceptance");
                return;
            }
            
            // Accept task
            localTasks.put(task.getTaskId(), task);
            currentTaskCount.incrementAndGet();
            task.markAssigned(localPeerInfo.getNodeId());
            
            log.info("Accepted data collection task {} for local execution", task.getTaskId());
            sendTaskResponse(sourceNodeId, task.getTaskId(), true, "Task accepted");
            
            // Execute task locally
            executeLocalTask(task);
            
        } catch (Exception e) {
            log.error("Error handling data collection task request", e);
        }
    }
    
    private void handleDataCollectionTaskResponse(SignedMessage message) {
        // Delegate to data collection router
        dataCollectionRouter.getActiveTasks(); // This would trigger the router's handler
        log.debug("Received data collection task response");
    }
    
    private void handleDataCollectionTaskResult(SignedMessage message) {
        try {
            DataCollectionTaskMessage result = gson.fromJson(message.getPayload(), DataCollectionTaskMessage.class);
            
            if (result == null) {
                log.warn("Received null data collection task result");
                return;
            }
            
            log.info("Received data collection task result: {} (status: {})", 
                    result.getTaskId(), result.getStatus());
            
            // Update local tracking
            if (result.getStatus() == DataCollectionTaskMessage.TaskStatus.COMPLETED) {
                totalTasksCompleted.incrementAndGet();
                bandwidthManager.recordDataUsage(result.getTargetUrl(), result.getDataCollectedMB());
            }
            
            // Remove from local tasks if it's ours
            localTasks.remove(result.getTaskId());
            
        } catch (Exception e) {
            log.error("Error handling data collection task result", e);
        }
    }
    
    private void handleDataCollectionTaskProgress(SignedMessage message) {
        try {
            DataCollectionTaskMessage progress = gson.fromJson(message.getPayload(), DataCollectionTaskMessage.class);
            
            if (progress == null) {
                log.warn("Received null data collection task progress");
                return;
            }
            
            log.debug("Received data collection task progress: {} ({}%)", 
                    progress.getTaskId(), progress.getProgressPercentage());
            
            // Update local task if it's ours
            DataCollectionTaskMessage localTask = localTasks.get(progress.getTaskId());
            if (localTask != null) {
                localTask.updateProgress(progress.getProgressPercentage(), progress.getDataCollectedMB());
            }
            
        } catch (Exception e) {
            log.error("Error handling data collection task progress", e);
        }
    }
    
    private void handlePassiveIncomePeerAdvertisement(SignedMessage message) {
        try {
            log.debug("Handling passive income peer advertisement from payload: {}", message.getPayload());
            System.out.println("DEBUG: [Bootstrap] handlePassiveIncomePeerAdvertisement payload=" + message.getPayload());
            PassiveIncomePeerInfo peerInfo = gson.fromJson(message.getPayload(), PassiveIncomePeerInfo.class);

            if (peerInfo == null) {
                log.warn("Received null passive income peer advertisement");
                return;
            }

            log.info("Received passive income peer advertisement from {} (wifi: {}, tasks: {}, reputation: {})",
                    peerInfo.getNodeId(), peerInfo.isWifiOnly(), peerInfo.getMaxConcurrentTasks(),
                    peerInfo.getReputationScore());

            // Update peer info in discovery service - use the correct method for remote peers
            // We need to access the private method via reflection or create a public wrapper
            // For now, let's use the handleResourceUpdate method which calls updatePassiveIncomePeerInfo
            SignedMessage resourceUpdate = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_RESOURCE_UPDATE)
                    .payload(message.getPayload())
                    .signature(message.getSignature())
                    .build();

            String sourceNodeId = extractNodeIdFromMessage(message);
            if (sourceNodeId != null) {
                log.debug("Passing resource update to discovery service for node {}", sourceNodeId);
                System.out.println("DEBUG: [Bootstrap] Passing resource update to discovery service for node " + sourceNodeId);
                passiveIncomeDiscoveryService.handleResourceUpdate(resourceUpdate, sourceNodeId);
            }

        } catch (Exception e) {
            log.error("Error handling passive income peer advertisement", e);
        }
    }
    
    private void handlePassiveIncomePeerDiscovery(SignedMessage message) {
        // Send back our passive income peer info
        try {
            String sourceNodeId = extractNodeIdFromMessage(message);
            if (sourceNodeId != null) {
                log.debug("Received passive income peer discovery request from {}", sourceNodeId);
                System.out.println("DEBUG: [Bootstrap] Received peer discovery request from " + sourceNodeId);
                
                // Send our passive income peer info
                SignedMessage response = SignedMessage.builder()
                        .type(MessageType.DATA_COLLECTION_PEER_ADVERTISEMENT)
                        .payload(gson.toJson(localPeerInfo))
                        .signature("") // Will be signed
                        .build();
                
                System.out.println("DEBUG: [Bootstrap] Sending peer advertisement to " + sourceNodeId + " with nodeId=" + localPeerInfo.getNodeId());
                super.sendMessage(sourceNodeId, response);
            }
        } catch (Exception e) {
            log.error("Error handling passive income peer discovery", e);
        }
    }
    
    private void handlePassiveIncomeResourceUpdate(SignedMessage message) {
        try {
            PassiveIncomePeerInfo peerInfo = gson.fromJson(message.getPayload(), PassiveIncomePeerInfo.class);
            
            if (peerInfo == null) {
                log.warn("Received null passive income resource update");
                return;
            }
            
            String sourceNodeId = extractNodeIdFromMessage(message);
            log.debug("Received passive income resource update from {}", sourceNodeId);
            System.out.println("DEBUG: [Bootstrap] Received resource update from " + sourceNodeId + " for node " + peerInfo.getNodeId());
            
            // Update peer resources in discovery service using the correct method
            if (sourceNodeId != null) {
                System.out.println("DEBUG: [Bootstrap] Forwarding resource update to discovery service for " + sourceNodeId);
                passiveIncomeDiscoveryService.handleResourceUpdate(message, sourceNodeId);
            }
            
        } catch (Exception e) {
            log.error("Error handling passive income resource update", e);
        }
    }
    
    private boolean canAcceptLocalTask(DataCollectionTaskMessage task) {
        // Check concurrent task limit
        if (currentTaskCount.get() >= maxConcurrentTasks) {
            log.warn("Cannot accept task {}: concurrent task limit reached ({}/{})", 
                    task.getTaskId(), currentTaskCount.get(), maxConcurrentTasks);
            return false;
        }
        
        // Check if we support the data type
        boolean supportsType = false;
        for (String supportedType : localPeerInfo.getSupportedDataTypes()) {
            if (supportedType.equals(task.getTaskType())) {
                supportsType = true;
                break;
            }
        }
        
        if (!supportsType) {
            log.warn("Cannot accept task {}: unsupported data type '{}'", 
                    task.getTaskId(), task.getTaskType());
            return false;
        }
        
        return true;
    }
    
    private void executeLocalTask(DataCollectionTaskMessage task) {
        // Simulate task execution
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting local execution of data collection task: {} (type: {})", 
                        task.getTaskId(), task.getTaskType());
                
                // Simulate progress
                for (int progress = 0; progress <= 100; progress += 25) {
                    Thread.sleep(1000); // Simulate work
                    
                    task.updateProgress(progress, progress * 10); // Simulate data collection
                    
                    // Send progress update back to requester
                    if (progress < 100) {
                        sendTaskProgress(task);
                    }
                }
                
                // Mark as completed
                task.updateProgress(100, 1000); // 100% progress, 1000MB collected
                
                log.info("Completed local execution of data collection task: {}", task.getTaskId());
                
                // Send result back to requester
                sendTaskResult(task);
                
                // Update local tracking
                totalTasksCompleted.incrementAndGet();
                bandwidthManager.recordDataUsage(task.getTargetUrl(), task.getDataCollectedMB());
                
            } catch (Exception e) {
                log.error("Error executing local data collection task: {}", task.getTaskId(), e);
                task.markFailed("Local execution failed: " + e.getMessage());
                sendTaskResult(task);
            } finally {
                currentTaskCount.decrementAndGet();
                localTasks.remove(task.getTaskId());
            }
        });
    }
    
    private void sendTaskResponse(String targetNodeId, String taskId, boolean accepted, String message) {
        try {
            DataCollectionTaskMessage response = new DataCollectionTaskMessage();
            response.setTaskId(taskId);
            response.setStatus(accepted ? DataCollectionTaskMessage.TaskStatus.ASSIGNED : DataCollectionTaskMessage.TaskStatus.CANCELLED);
            response.setErrorMessage(message);
            
            SignedMessage signedResponse = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_TASK_RESPONSE)
                    .payload(gson.toJson(response))
                    .signature("") // Will be signed
                    .build();
            
            super.sendMessage(targetNodeId, signedResponse);
            
        } catch (Exception e) {
            log.error("Error sending task response", e);
        }
    }
    
    private void sendTaskProgress(DataCollectionTaskMessage task) {
        try {
            SignedMessage progressMessage = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_TASK_PROGRESS)
                    .payload(gson.toJson(task))
                    .signature("") // Will be signed
                    .build();
            
            // Send to the requester (this would need to be tracked)
            // For now, broadcast to all peers
            super.broadcastMessage(progressMessage);
            
        } catch (Exception e) {
            log.error("Error sending task progress", e);
        }
    }
    
    private void sendTaskResult(DataCollectionTaskMessage task) {
        try {
            SignedMessage resultMessage = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_TASK_RESULT)
                    .payload(gson.toJson(task))
                    .signature("") // Will be signed
                    .build();
            
            // Send to the requester (this would need to be tracked)
            // For now, broadcast to all peers
            super.broadcastMessage(resultMessage);
            
        } catch (Exception e) {
            log.error("Error sending task result", e);
        }
    }
    
    private String extractNodeIdFromMessage(SignedMessage msg) {
        try {
            // Try to parse as PassiveIncomePeerInfo first (for passive income messages)
            PassiveIncomePeerInfo passiveInfo = gson.fromJson(msg.getPayload(), PassiveIncomePeerInfo.class);
            if (passiveInfo != null && passiveInfo.getNodeId() != null) {
                return passiveInfo.getNodeId();
            }
            
            // Fall back to PeerInfo for other message types
            PeerInfo info = gson.fromJson(msg.getPayload(), PeerInfo.class);
            return info != null ? info.getNodeId() : null;
        } catch (Exception e) {
            log.warn("Failed to extract nodeId from message payload: {}", e.getMessage());
            return null;
        }
    }
    
    // Public API methods
    
    public CompletableFuture<DataCollectionTaskMessage> submitDataCollectionTask(DataCollectionTaskMessage task) {
        CompletableFuture<DataCollectionTaskMessage> future = new CompletableFuture<>();
        
        dataCollectionRouter.submitDataCollectionTask(task, new DataCollectionMessageRouter.TaskResultCallback() {
            @Override
            public void onTaskCompleted(String taskId, DataCollectionTaskMessage result) {
                future.complete(result);
            }
            
            @Override
            public void onTaskFailed(String taskId, String errorMessage) {
                future.completeExceptionally(new RuntimeException(errorMessage));
            }
            
            @Override
            public void onTaskProgress(String taskId, int progress, long dataCollectedMB) {
                // Could be used to update UI or logging
                log.debug("Task {} progress: {}% ({}MB collected)", taskId, progress, dataCollectedMB);
            }
        });
        
        return future;
    }
    
    public PassiveIncomeBandwidthManager.BandwidthStatus getBandwidthStatus() {
        return bandwidthManager.getCurrentStatus();
    }
    
    public PassiveIncomePeerInfo getLocalPassiveIncomeInfo() {
        return localPeerInfo;
    }
    
    public PassiveIncomePeerInfo getPassiveIncomePeerInfo(String nodeId) {
        PassiveIncomePeerInfo info = passiveIncomeDiscoveryService.getPassiveIncomePeerInfo(nodeId);
        log.debug("getPassiveIncomePeerInfo for {} returned: {}", nodeId, info != null ? info.getNodeId() : "null");
        return info;
    }
    
    public int getCurrentTaskCount() {
        return currentTaskCount.get();
    }
    
    public int getTotalTasksCompleted() {
        return totalTasksCompleted.get();
    }
    
    public void updateLocalResources(int batteryLevel, boolean charging, boolean onWifi) {
        localPeerInfo.setBatteryLevel(batteryLevel);
        localPeerInfo.setCharging(charging);
        bandwidthManager.setCurrentlyOnWifi(onWifi);
        
        // Broadcast resource update
        broadcastResourceUpdate();
    }
    
    private void broadcastResourceUpdate() {
        try {
            SignedMessage resourceUpdate = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_RESOURCE_UPDATE)
                    .payload(gson.toJson(localPeerInfo))
                    .signature("") // Will be signed
                    .build();
            
            System.out.println("DEBUG: [Bootstrap] Broadcasting resource update for node=" + localPeerInfo.getNodeId());
            super.broadcastMessage(resourceUpdate);
            
        } catch (Exception e) {
            log.error("Error broadcasting resource update", e);
        }
    }
    
    @Override
    public void start() throws InterruptedException {
        log.info("Starting PassiveIncomeP2PNodeBootstrap");
        
        // Start base services
        super.start();
        
        // Update discovery service with local peer info
        passiveIncomeDiscoveryService.updateLocalPeerResources(localPeerInfo);
        
        // Start passive income discovery service
        passiveIncomeDiscoveryService.start();
        
        // Broadcast our passive income capabilities after a delay to ensure connections are established
        CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS).execute(() -> {
            try {
                broadcastPassiveIncomeAdvertisement();
            } catch (Exception e) {
                log.error("Error broadcasting initial passive income advertisement", e);
            }
        });
        
        log.info("PassiveIncomeP2PNodeBootstrap started successfully");
        log.info("Passive income capabilities - WiFi-only: {}, daily limit: {}MB, max tasks: {}",
                wifiOnly, maxDailyDataUsageMB, maxConcurrentTasks);
    }
    
    private void broadcastPassiveIncomeAdvertisement() {
        try {
            SignedMessage advertisement = SignedMessage.builder()
                    .type(MessageType.DATA_COLLECTION_PEER_ADVERTISEMENT)
                    .payload(gson.toJson(localPeerInfo))
                    .signature("") // Will be signed
                    .build();
            
            System.out.println("DEBUG: [Bootstrap] Broadcasting peer advertisement for node=" + localPeerInfo.getNodeId());
            super.broadcastMessage(advertisement);
            
            log.info("Broadcasted passive income peer advertisement");
            
        } catch (Exception e) {
            log.error("Error broadcasting passive income advertisement", e);
        }
    }
    
    @Override
    public void stop() {
        log.info("Stopping PassiveIncomeP2PNodeBootstrap");
        
        // Stop passive income services
        passiveIncomeDiscoveryService.stop();
        bandwidthManager.shutdown();
        
        // Stop base services
        super.stop();
        
        log.info("PassiveIncomeP2PNodeBootstrap stopped");
    }
    
    // Inner class to implement PassiveIncomePeerDiscoveryService.PeerAdvertisementSender
    private class PassiveIncomePeerAdvertisementSenderImpl implements PassiveIncomePeerDiscoveryService.PeerAdvertisementSender {
        @Override
        public void sendAdvertisement(String targetNodeId, SignedMessage advertisement) {
            sendMessage(targetNodeId, advertisement);
        }

        @Override
        public void sendDiscoveryRequest(String targetNodeId, SignedMessage discoveryRequest) {
            sendMessage(targetNodeId, discoveryRequest);
        }
    }
}