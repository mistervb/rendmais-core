package br.com.rendmais.p2p.messaging;

import br.com.rendmais.common.dto.DataCollectionTaskMessage;
import br.com.rendmais.common.dto.PassiveIncomePeerInfo;
import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.discovery.PassiveIncomePeerDiscoveryService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataCollectionMessageRouter {
    
    private static final Logger log = LoggerFactory.getLogger(DataCollectionMessageRouter.class);
    private static final long TASK_TIMEOUT_MINUTES = 30;
    private static final int MAX_CONCURRENT_TASKS_PER_PEER = 2;
    
    private final MessageRouter baseRouter;
    private final PassiveIncomePeerDiscoveryService discoveryService;
    private final Gson gson = new Gson();
    
    // Task tracking
    private final Map<String, DataCollectionTaskMessage> activeTasks = new ConcurrentHashMap<>();
    private final Map<String, String> taskAssignments = new ConcurrentHashMap<>(); // taskId -> nodeId
    private final Map<String, AtomicInteger> peerTaskCounts = new ConcurrentHashMap<>(); // nodeId -> task count
    private final ScheduledExecutorService taskScheduler;
    
    // Task result callbacks
    private final Map<String, TaskResultCallback> taskCallbacks = new ConcurrentHashMap<>();
    
    public interface TaskResultCallback {
        void onTaskCompleted(String taskId, DataCollectionTaskMessage result);
        void onTaskFailed(String taskId, String errorMessage);
        void onTaskProgress(String taskId, int progress, long dataCollectedMB);
    }
    
    public DataCollectionMessageRouter(MessageRouter baseRouter, 
                                     PassiveIncomePeerDiscoveryService discoveryService) {
        this.baseRouter = baseRouter;
        this.discoveryService = discoveryService;
        
        this.taskScheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "data-collection-task-monitor");
            t.setDaemon(true);
            return t;
        });
        
        registerDataCollectionHandlers();
        startTaskMonitoring();
    }
    
    private void registerDataCollectionHandlers() {
        // Data collection task request handler
        baseRouter.registerHandler(MessageType.DATA_COLLECTION_TASK_REQUEST, message -> {
            handleDataCollectionTaskRequest(message);
        });
        
        // Data collection task response handler
        baseRouter.registerHandler(MessageType.DATA_COLLECTION_TASK_RESPONSE, message -> {
            handleDataCollectionTaskResponse(message);
        });
        
        // Data collection task result handler
        baseRouter.registerHandler(MessageType.DATA_COLLECTION_TASK_RESULT, message -> {
            handleDataCollectionTaskResult(message);
        });
        
        // Data collection task progress handler
        baseRouter.registerHandler(MessageType.DATA_COLLECTION_TASK_PROGRESS, message -> {
            handleDataCollectionTaskProgress(message);
        });
        
        // Data collection resource update handler
        baseRouter.registerHandler(MessageType.DATA_COLLECTION_RESOURCE_UPDATE, message -> {
            if (discoveryService != null) {
                String sourceNodeId = extractNodeIdFromMessage(message);
                if (sourceNodeId != null) {
                    discoveryService.handleResourceUpdate(message, sourceNodeId);
                }
            }
        });
    }
    
    public void submitDataCollectionTask(DataCollectionTaskMessage task, TaskResultCallback callback) {
        if (!task.isValidForPassiveIncome()) {
            log.warn("Task {} is not valid for passive income execution", task.getTaskId());
            if (callback != null) {
                callback.onTaskFailed(task.getTaskId(), "Task not valid for passive income");
            }
            return;
        }
        
        // Store task and callback
        activeTasks.put(task.getTaskId(), task);
        if (callback != null) {
            taskCallbacks.put(task.getTaskId(), callback);
        }
        
        // Find suitable peers
        List<PassiveIncomePeerInfo> suitablePeers = discoveryService.findSuitablePeersForTask(task);
        
        if (suitablePeers.isEmpty()) {
            log.warn("No suitable peers found for task {}", task.getTaskId());
            if (callback != null) {
                callback.onTaskFailed(task.getTaskId(), "No suitable peers available");
            }
            cleanupTask(task.getTaskId());
            return;
        }
        
        // Select best peer (first in sorted list)
        PassiveIncomePeerInfo selectedPeer = suitablePeers.get(0);
        String selectedNodeId = selectedPeer.getNodeId();
        
        // Check if peer has capacity
        AtomicInteger peerTaskCount = peerTaskCounts.computeIfAbsent(selectedNodeId, k -> new AtomicInteger(0));
        if (peerTaskCount.get() >= MAX_CONCURRENT_TASKS_PER_PEER) {
            log.warn("Peer {} has reached maximum concurrent tasks", selectedNodeId);
            if (callback != null) {
                callback.onTaskFailed(task.getTaskId(), "Selected peer has no capacity");
            }
            cleanupTask(task.getTaskId());
            return;
        }
        
        // Assign task to peer
        taskAssignments.put(task.getTaskId(), selectedNodeId);
        peerTaskCount.incrementAndGet();
        task.markAssigned(selectedNodeId);
        
        log.info("Assigned data collection task {} to peer {} (type: {}, url: {})", 
                task.getTaskId(), selectedNodeId, task.getTaskType(), task.getTargetUrl());
        
        // Send task to peer
        SignedMessage taskMessage = SignedMessage.builder()
                .type(MessageType.DATA_COLLECTION_TASK_REQUEST)
                .payload(gson.toJson(task))
                .signature("") // Will be signed by sender
                .build();
        
        // This would need to be implemented to send to specific peer
        log.debug("Sending data collection task {} to peer {}", task.getTaskId(), selectedNodeId);
    }
    
    private void handleDataCollectionTaskRequest(SignedMessage message) {
        try {
            DataCollectionTaskMessage task = gson.fromJson(message.getPayload(), DataCollectionTaskMessage.class);
            
            if (task == null) {
                log.warn("Received null data collection task request");
                return;
            }
            
            log.info("Received data collection task request: {} (type: {})", task.getTaskId(), task.getTaskType());
            
            // Validate task
            if (!task.isValidForPassiveIncome()) {
                log.warn("Task {} is not valid for passive income", task.getTaskId());
                sendTaskResponse(task.getTaskId(), false, "Task not valid for passive income");
                return;
            }
            
            // Check if we can accept this task
            if (!canAcceptTask(task)) {
                log.warn("Cannot accept data collection task {} due to resource constraints", task.getTaskId());
                sendTaskResponse(task.getTaskId(), false, "Resource constraints prevent task acceptance");
                return;
            }
            
            // Accept task
            activeTasks.put(task.getTaskId(), task);
            task.markAssigned("local"); // Mark as assigned to local node
            
            log.info("Accepted data collection task {} for execution", task.getTaskId());
            sendTaskResponse(task.getTaskId(), true, "Task accepted");
            
            // Here you would typically delegate to a task executor
            // For now, we'll simulate task execution
            simulateTaskExecution(task);
            
        } catch (Exception e) {
            log.error("Error handling data collection task request", e);
        }
    }
    
    private void handleDataCollectionTaskResponse(SignedMessage message) {
        // Handle task acceptance/rejection responses
        log.debug("Received data collection task response");
    }
    
    private void handleDataCollectionTaskResult(SignedMessage message) {
        try {
            DataCollectionTaskMessage result = gson.fromJson(message.getPayload(), DataCollectionTaskMessage.class);
            
            if (result == null) {
                log.warn("Received null data collection task result");
                return;
            }
            
            log.info("Received data collection task result: {} (status: {})", result.getTaskId(), result.getStatus());
            
            // Notify callback
            TaskResultCallback callback = taskCallbacks.get(result.getTaskId());
            if (callback != null) {
                if (result.getStatus() == DataCollectionTaskMessage.TaskStatus.COMPLETED) {
                    callback.onTaskCompleted(result.getTaskId(), result);
                } else if (result.getStatus() == DataCollectionTaskMessage.TaskStatus.FAILED) {
                    callback.onTaskFailed(result.getTaskId(), result.getErrorMessage());
                }
            }
            
            // Cleanup task
            cleanupTask(result.getTaskId());
            
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
            
            log.debug("Received data collection task progress: {} ({}%)", progress.getTaskId(), progress.getProgressPercentage());
            
            // Notify callback
            TaskResultCallback callback = taskCallbacks.get(progress.getTaskId());
            if (callback != null) {
                callback.onTaskProgress(progress.getTaskId(), progress.getProgressPercentage(), progress.getDataCollectedMB());
            }
            
        } catch (Exception e) {
            log.error("Error handling data collection task progress", e);
        }
    }
    
    private boolean canAcceptTask(DataCollectionTaskMessage task) {
        // This would check local resources, battery, WiFi, etc.
        // For now, always accept valid tasks
        return true;
    }
    
    private void sendTaskResponse(String taskId, boolean accepted, String message) {
        // Send task response back to requester
        log.debug("Sending task response for {}: accepted={}, message={}", taskId, accepted, message);
    }
    
    private void simulateTaskExecution(DataCollectionTaskMessage task) {
        // Simulate task execution with progress updates
        taskScheduler.schedule(() -> {
            try {
                // Simulate progress
                for (int i = 0; i <= 100; i += 25) {
                    final int progress = i;
                    taskScheduler.schedule(() -> {
                        task.updateProgress(progress, progress * 10); // Simulate data collection
                        log.info("Simulating task {} progress: {}%", task.getTaskId(), progress);
                        
                        // Send progress update
                        if (progress < 100) {
                            // This would send progress back to requester
                        }
                    }, progress / 25, TimeUnit.SECONDS);
                }
                
                // Mark as completed after simulation
                taskScheduler.schedule(() -> {
                    task.updateProgress(100, 1000); // 100% progress, 1000MB collected
                    log.info("Simulated task {} completed", task.getTaskId());
                    
                    // Send result back to requester
                    // This would send the final result
                    
                    // Cleanup
                    cleanupTask(task.getTaskId());
                }, 5, TimeUnit.SECONDS);
                
            } catch (Exception e) {
                log.error("Error simulating task execution", e);
                task.markFailed("Simulation error: " + e.getMessage());
            }
        }, 0, TimeUnit.SECONDS);
    }
    
    private void startTaskMonitoring() {
        // Monitor for timed out tasks
        taskScheduler.scheduleAtFixedRate(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                long timeoutMillis = TASK_TIMEOUT_MINUTES * 60 * 1000;
                
                Iterator<Map.Entry<String, DataCollectionTaskMessage>> iterator = activeTasks.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, DataCollectionTaskMessage> entry = iterator.next();
                    DataCollectionTaskMessage task = entry.getValue();
                    
                    if (task.getStartedTimestamp() > 0 && 
                        (currentTime - task.getStartedTimestamp()) > timeoutMillis) {
                        
                        log.warn("Task {} timed out after {} minutes", task.getTaskId(), TASK_TIMEOUT_MINUTES);
                        task.markFailed("Task timed out");
                        
                        // Notify callback
                        TaskResultCallback callback = taskCallbacks.get(task.getTaskId());
                        if (callback != null) {
                            callback.onTaskFailed(task.getTaskId(), "Task timed out");
                        }
                        
                        iterator.remove();
                        cleanupTask(task.getTaskId());
                    }
                }
            } catch (Exception e) {
                log.error("Error during task monitoring", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }
    
    private void cleanupTask(String taskId) {
        activeTasks.remove(taskId);
        taskCallbacks.remove(taskId);
        
        // Decrement peer task count
        String assignedNodeId = taskAssignments.remove(taskId);
        if (assignedNodeId != null) {
            AtomicInteger taskCount = peerTaskCounts.get(assignedNodeId);
            if (taskCount != null) {
                taskCount.decrementAndGet();
            }
        }
    }
    
    private String extractNodeIdFromMessage(SignedMessage msg) {
        // This would extract node ID from message metadata or routing info
        return "unknown";
    }
    
    public int getActiveTaskCount() {
        return activeTasks.size();
    }
    
    public Map<String, DataCollectionTaskMessage> getActiveTasks() {
        return new HashMap<>(activeTasks);
    }
}