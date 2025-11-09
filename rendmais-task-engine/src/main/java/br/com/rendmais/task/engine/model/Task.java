package br.com.rendmais.task.engine.model;

import lombok.*;

import java.time.Instant;
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