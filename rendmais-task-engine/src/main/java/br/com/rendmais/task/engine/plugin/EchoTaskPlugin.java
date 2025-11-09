package br.com.rendmais.task.engine.plugin;

import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EchoTaskPlugin implements TaskPlugin {
    
    private static final Logger log = LoggerFactory.getLogger(EchoTaskPlugin.class);
    
    private static final String PLUGIN_ID = "echo-plugin";
    private static final String PLUGIN_NAME = "Echo Task Plugin";
    private static final String PLUGIN_VERSION = "1.0.0";
    
    private volatile boolean isHealthy = true;
    private Map<String, String> config;
    
    @Override
    public String getPluginId() {
        return PLUGIN_ID;
    }
    
    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
    
    @Override
    public String getPluginVersion() {
        return PLUGIN_VERSION;
    }
    
    @Override
    public Set<String> getSupportedTaskTypes() {
        return Set.of("ECHO", "PING", "HELLO");
    }
    
    @Override
    public TaskResult execute(Task task) throws Exception {
        log.info("Executing {} task: {}", task.getTaskType(), task.getTaskId());
        
        try {
            // Simulate some processing time
            Thread.sleep(100);
            
            String result = switch (task.getTaskType()) {
                case "ECHO" -> "Echo: " + task.getPayload();
                case "PING" -> "Pong: " + task.getPayload();
                case "HELLO" -> "Hello, " + task.getPayload() + "!";
                default -> throw new IllegalArgumentException("Unsupported task type: " + task.getTaskType());
            };
            
            log.info("Task {} completed successfully", task.getTaskId());
            return TaskResult.success(task.getTaskId(), result, "node-1");
            
        } catch (InterruptedException e) {
            log.error("Task {} was interrupted", task.getTaskId(), e);
            return TaskResult.failure(task.getTaskId(), "Task was interrupted: " + e.getMessage(), "node-1");
            
        } catch (Exception e) {
            log.error("Task {} failed with error", task.getTaskId(), e);
            return TaskResult.failure(task.getTaskId(), "Task failed: " + e.getMessage(), "node-1");
        }
    }
    
    @Override
    public void initialize(Map<String, String> config) {
        this.config = new HashMap<>(config);
        log.info("Initialized {} with config: {}", PLUGIN_NAME, config);
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down {}", PLUGIN_NAME);
        this.config = null;
    }
    
    @Override
    public boolean isHealthy() {
        return isHealthy;
    }
    
    @Override
    public void validateTask(Task task) throws IllegalArgumentException {
        if (task == null || task.getPayload() == null) {
            throw new IllegalArgumentException("Invalid task: null task or payload");
        }
        
        if (task.getPayload().trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid task: empty payload");
        }
    }
    
    @Override
    public Map<String, Object> getPluginInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("description", "Simple echo task plugin for testing");
        info.put("config", config != null ? config : new HashMap<>());
        info.put("healthCheck", isHealthy);
        return info;
    }
}