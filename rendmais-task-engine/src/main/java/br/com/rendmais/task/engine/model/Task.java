package br.com.rendmais.task.engine.model;

import lombok.*;

import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Task {
    private String taskId;
    private String taskType;
    private String pluginId;
    private TaskPriority priority;
    private TaskStatus status;
    private String payload;
    private Map<String, String> metadata;
    private String nodeId;
    private Instant createdAt;
    private Instant scheduledAt;
    private Instant startedAt;
    private Instant completedAt;
    private String result;
    private String errorMessage;
    private int retryCount;
    private int maxRetries;
    
    // Passive income specific fields
    private String targetUrl; // URL to collect data from
    private String dataType; // Type of data to collect (price, product, review, etc.)
    private Duration estimatedDuration; // Estimated time for data collection
    private long dataSize; // Expected data size in bytes
    private boolean requiresWifi; // Whether task requires WiFi
    private int maxRequests; // Maximum number of HTTP requests
    private String collectionPattern; // Pattern for data extraction

    public static Task create(String taskType, String pluginId, String payload, TaskPriority priority) {
        return Task.builder()
                .taskId(UUID.randomUUID().toString())
                .taskType(taskType)
                .pluginId(pluginId)
                .priority(priority)
                .status(TaskStatus.PENDING)
                .payload(payload)
                .createdAt(Instant.now())
                .retryCount(0)
                .maxRetries(3)
                .build();
    }

    public static Task createScheduled(String taskType, String pluginId, String payload, 
                                     TaskPriority priority, Instant scheduledAt) {
        Task task = create(taskType, pluginId, payload, priority);
        task.setScheduledAt(scheduledAt);
        return task;
    }
    
    // Passive income data collection task factories
    public static Task createDataCollectionTask(String taskType, String pluginId, String targetUrl, 
                                              String dataType, String collectionPattern) {
        return Task.builder()
                .taskId(UUID.randomUUID().toString())
                .taskType(taskType)
                .pluginId(pluginId)
                .targetUrl(targetUrl)
                .dataType(dataType)
                .collectionPattern(collectionPattern)
                .priority(TaskPriority.LOW) // Low priority for passive operation
                .status(TaskStatus.PENDING)
                .requiresWifi(true) // Always require WiFi
                .maxRequests(10) // Limit requests to save data
                .dataSize(1024 * 1024) // Default 1MB max
                .estimatedDuration(Duration.ofMinutes(5))
                .createdAt(Instant.now())
                .retryCount(0)
                .maxRetries(2) // Reduced retries
                .build();
    }
    
    public static Task createPriceCollectionTask(String pluginId, String targetUrl, String collectionPattern) {
        return createDataCollectionTask("PRICE_COLLECTION", pluginId, targetUrl, "price", collectionPattern);
    }
    
    public static Task createProductInfoTask(String pluginId, String targetUrl, String collectionPattern) {
        return createDataCollectionTask("PRODUCT_INFO", pluginId, targetUrl, "product_info", collectionPattern);
    }
    
    public static Task createMarketResearchTask(String pluginId, String targetUrl, String collectionPattern) {
        return createDataCollectionTask("MARKET_RESEARCH", pluginId, targetUrl, "market_data", collectionPattern);
    }

    public void markAsRunning(String nodeId) {
        this.status = TaskStatus.RUNNING;
        this.nodeId = nodeId;
        this.startedAt = Instant.now();
    }

    public void markAsCompleted(String result) {
        this.status = TaskStatus.COMPLETED;
        this.result = result;
        this.completedAt = Instant.now();
    }

    public void markAsFailed(String errorMessage) {
        this.status = TaskStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completedAt = Instant.now();
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public boolean canRetry() {
        return retryCount < maxRetries;
    }

    public boolean isScheduled() {
        return scheduledAt != null && scheduledAt.isAfter(Instant.now());
    }

    public boolean isReadyToExecute() {
        return status == TaskStatus.PENDING && 
               (scheduledAt == null || scheduledAt.isBefore(Instant.now()));
    }
}