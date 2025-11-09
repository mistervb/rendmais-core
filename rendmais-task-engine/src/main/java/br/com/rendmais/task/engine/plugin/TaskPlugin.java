package br.com.rendmais.task.engine.plugin;

import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskResult;

import java.util.Map;
import java.util.Set;

public interface TaskPlugin {
    
    String getPluginId();
    
    String getPluginName();
    
    String getPluginVersion();
    
    Set<String> getSupportedTaskTypes();
    
    TaskResult execute(Task task) throws Exception;
    
    void initialize(Map<String, String> config) throws Exception;
    
    void shutdown() throws Exception;
    
    boolean isHealthy();
    
    Map<String, Object> getPluginInfo();
    
    default boolean supportsTaskType(String taskType) {
        return getSupportedTaskTypes().contains(taskType);
    }
    
    default void validateTask(Task task) throws IllegalArgumentException {
        if (!supportsTaskType(task.getTaskType())) {
            throw new IllegalArgumentException(
                String.format("Plugin %s does not support task type: %s", 
                    getPluginId(), task.getTaskType())
            );
        }
    }
}