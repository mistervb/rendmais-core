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
    public TaskResult execute(Task task) {
        log.info("Executing {} task: {}", task.getType(), task.getId());
        
        try {
            String payload = task.getPayload();
            String response = "Echo: " + payload;
            
            // Simulate some processing time
            Thread.sleep(100);
            
            return TaskResult.success(task.getId(), response);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure(task.getId(), "Task interrupted", e);
        } catch (Exception e) {
            return TaskResult.failure(task.getId(), "Echo task failed", e);
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
    public boolean validateTask(Task task) {
        if (task == null || task.getPayload() == null) {
            log.warn("Invalid task: null task or payload");
            return false;
        }
        
        if (task.getPayload().length() > 1000) {
            log.warn("Task payload too large: {} characters", task.getPayload().length());
            return false;
        }
        
        return true;
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