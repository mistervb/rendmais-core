package br.com.rendmais.task.engine.p2p;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskResult;
import br.com.rendmais.task.engine.executor.TaskExecutor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TaskP2PIntegration {
    
    private static final Logger log = LoggerFactory.getLogger(TaskP2PIntegration.class);
    
    private final MessageRouter messageRouter;
    private final TaskExecutor taskExecutor;
    private final Gson gson;
    private final Map<String, Consumer<TaskResult>> resultCallbacks = new ConcurrentHashMap<>();
    
    public TaskP2PIntegration(MessageRouter messageRouter, TaskExecutor taskExecutor) {
        this.messageRouter = messageRouter;
        this.taskExecutor = taskExecutor;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        
        registerTaskHandlers();
    }
    
    private void registerTaskHandlers() {
        // Register handler for TASK_REQUEST messages
        messageRouter.registerHandler(MessageType.TASK_REQUEST, this::handleTaskRequest);
        
        // Register handler for TASK_RESULT messages
        messageRouter.registerHandler(MessageType.TASK_RESULT, this::handleTaskResult);
    }
    
    private void handleTaskRequest(SignedMessage message) {
        try {
            String payload = message.getPayload();
            TaskRequestMessage request = gson.fromJson(payload, TaskRequestMessage.class);
            String peerId = "unknown";
            
            log.debug("Received task request from {}: {}", peerId, request.getTaskId());
            
            // Process task request (implementation will be provided by TaskEngine)
            // For now, just log it
            log.info("Task request received: {} from peer {}", request.getTaskId(), peerId);
            
        } catch (Exception e) {
            String peerId = "unknown";
            log.error("Error handling task request from {}", peerId, e);
        }
    }
    
    private void handleTaskResult(SignedMessage message) {
        try {
            String payload = message.getPayload();
            TaskResultMessage result = gson.fromJson(payload, TaskResultMessage.class);
            String peerId = "unknown";
            
            log.debug("Received task result from {}: {}", peerId, result.getTaskId());
            
            // Notify callback if registered
            Consumer<TaskResult> callback = resultCallbacks.remove(result.getTaskId());
            if (callback != null) {
                callback.accept(result.getTaskResult());
            }
            
        } catch (Exception e) {
            String peerId = "unknown";
            log.error("Error handling task result from {}", peerId, e);
        }
    }
    
    public void broadcastTaskRequest(Task task, String targetPeerId) {
        try {
            TaskRequestMessage request = new TaskRequestMessage(
                task.getTaskId(),
                task.getTaskType(),
                task.getPayload(),
                task.getMetadata(),
                task.getPriority().name(),
                targetPeerId
            );
            
            String payload = gson.toJson(request);
            
            SignedMessage message = new SignedMessage();
            message.setType(MessageType.TASK_REQUEST);
            message.setPayload(payload);
            
            // The actual broadcasting will be handled by the P2P layer
            // This method just prepares the message
            log.info("Prepared task request for broadcasting: {} to peer {}", task.getTaskId(), targetPeerId);
            
        } catch (Exception e) {
            log.error("Error broadcasting task request: {}", task.getTaskId(), e);
        }
    }
    
    public void sendTaskResult(TaskResult result, String targetPeerId) {
        try {
            TaskResultMessage resultMessage = new TaskResultMessage(
                result.getTaskId(),
                result.getStatus().name(),
                result.getResult(),
                result.getErrorMessage(),
                result.getExecutionTime(),
                result.getNodeId()
            );
            
            String payload = gson.toJson(resultMessage);
            
            SignedMessage message = new SignedMessage();
            message.setType(MessageType.TASK_RESULT);
            message.setPayload(payload);
            
            // The actual sending will be handled by the P2P layer
            log.info("Prepared task result for sending: {} to peer {}", result.getTaskId(), targetPeerId);
            
        } catch (Exception e) {
            log.error("Error sending task result: {}", result.getTaskId(), e);
        }
    }
    
    public void registerResultCallback(String taskId, Consumer<TaskResult> callback) {
        resultCallbacks.put(taskId, callback);
    }
    
    public void unregisterResultCallback(String taskId) {
        resultCallbacks.remove(taskId);
    }
    
    public static class TaskRequestMessage {
        private String taskId;
        private String taskType;
        private String payload;
        private Map<String, String> metadata;
        private String priority;
        private String targetPeerId;
        
        public TaskRequestMessage() {}
        
        public TaskRequestMessage(String taskId, String taskType, String payload, 
                                Map<String, String> metadata, String priority, String targetPeerId) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.payload = payload;
            this.metadata = metadata;
            this.priority = priority;
            this.targetPeerId = targetPeerId;
        }
        
        public String getTaskId() { return taskId; }
        public void setTaskId(String taskId) { this.taskId = taskId; }
        
        public String getTaskType() { return taskType; }
        public void setTaskType(String taskType) { this.taskType = taskType; }
        
        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }
        
        public Map<String, String> getMetadata() { return metadata; }
        public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
        
        public String getPriority() { return priority; }
        public void setPriority(String priority) { this.priority = priority; }
        
        public String getTargetPeerId() { return targetPeerId; }
        public void setTargetPeerId(String targetPeerId) { this.targetPeerId = targetPeerId; }
    }
    
    public static class TaskResultMessage {
        private String taskId;
        private String status;
        private String result;
        private String errorMessage;
        private long executionTime;
        private String nodeId;
        
        public TaskResultMessage() {}
        
        public TaskResultMessage(String taskId, String status, String result, 
                               String errorMessage, long executionTime, String nodeId) {
            this.taskId = taskId;
            this.status = status;
            this.result = result;
            this.errorMessage = errorMessage;
            this.executionTime = executionTime;
            this.nodeId = nodeId;
        }
        
        public String getTaskId() { return taskId; }
        public void setTaskId(String taskId) { this.taskId = taskId; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public String getResult() { return result; }
        public void setResult(String result) { this.result = result; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public long getExecutionTime() { return executionTime; }
        public void setExecutionTime(long executionTime) { this.executionTime = executionTime; }
        
        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        
        public TaskResult getTaskResult() {
            return TaskResult.success(taskId, result, nodeId);
        }
    }
}