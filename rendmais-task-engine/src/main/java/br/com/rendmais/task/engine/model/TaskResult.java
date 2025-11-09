package br.com.rendmais.task.engine.model;

import lombok.*;

import java.time.Instant;
import java.util.Map;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TaskResult {
    private String taskId;
    private TaskStatus status;
    private String result;
    private String errorMessage;
    private Instant startTime;
    private Instant endTime;
    private String nodeId;
    private int retryCount;
    private Map<String, Object> metadata;

    public static TaskResult success(String taskId, String result, String nodeId) {
        Instant now = Instant.now();
        return TaskResult.builder()
                .taskId(taskId)
                .status(TaskStatus.COMPLETED)
                .result(result)
                .startTime(now)
                .endTime(now)
                .nodeId(nodeId)
                .build();
    }

    public static TaskResult failure(String taskId, String errorMessage, String nodeId) {
        Instant now = Instant.now();
        return TaskResult.builder()
                .taskId(taskId)
                .status(TaskStatus.FAILED)
                .errorMessage(errorMessage)
                .startTime(now)
                .endTime(now)
                .nodeId(nodeId)
                .build();
    }

    public long getExecutionTime() {
        if (startTime != null && endTime != null) {
            return java.time.Duration.between(startTime, endTime).toMillis();
        }
        return 0;
    }

    public boolean isSuccessful() {
        return status == TaskStatus.COMPLETED;
    }
}