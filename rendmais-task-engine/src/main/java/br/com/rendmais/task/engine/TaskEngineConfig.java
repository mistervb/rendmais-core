package br.com.rendmais.task.engine;

import java.util.HashMap;
import java.util.Map;

public class TaskEngineConfig {
    
    private int coreThreads = 2;
    private int maxThreads = 10;
    private long taskTimeout = 300; // 5 minutes in seconds
    private int maxRetries = 3;
    private boolean enableDistributedExecution = true;
    private boolean enableScheduling = true;
    private Map<String, Map<String, String>> pluginConfigs = new HashMap<>();
    
    public TaskEngineConfig() {}
    
    public TaskEngineConfig(int coreThreads, int maxThreads, long taskTimeout) {
        this.coreThreads = coreThreads;
        this.maxThreads = maxThreads;
        this.taskTimeout = taskTimeout;
    }
    
    public int getCoreThreads() {
        return coreThreads;
    }
    
    public void setCoreThreads(int coreThreads) {
        this.coreThreads = coreThreads;
    }
    
    public int getMaxThreads() {
        return maxThreads;
    }
    
    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }
    
    public long getTaskTimeout() {
        return taskTimeout;
    }
    
    public void setTaskTimeout(long taskTimeout) {
        this.taskTimeout = taskTimeout;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public boolean isEnableDistributedExecution() {
        return enableDistributedExecution;
    }
    
    public void setEnableDistributedExecution(boolean enableDistributedExecution) {
        this.enableDistributedExecution = enableDistributedExecution;
    }
    
    public boolean isEnableScheduling() {
        return enableScheduling;
    }
    
    public void setEnableScheduling(boolean enableScheduling) {
        this.enableScheduling = enableScheduling;
    }
    
    public Map<String, String> getPluginConfig(String pluginId) {
        return pluginConfigs.getOrDefault(pluginId, new HashMap<>());
    }
    
    public void setPluginConfig(String pluginId, Map<String, String> config) {
        pluginConfigs.put(pluginId, config);
    }
    
    public void addPluginConfig(String pluginId, String key, String value) {
        pluginConfigs.computeIfAbsent(pluginId, k -> new HashMap<>()).put(key, value);
    }
    
    public Map<String, Map<String, String>> getPluginConfigs() {
        return new HashMap<>(pluginConfigs);
    }
    
    public void setPluginConfigs(Map<String, Map<String, String>> pluginConfigs) {
        this.pluginConfigs = pluginConfigs;
    }
    
    public static TaskEngineConfig defaultConfig() {
        return new TaskEngineConfig();
    }
    
    public static TaskEngineConfig highPerformanceConfig() {
        TaskEngineConfig config = new TaskEngineConfig();
        config.setCoreThreads(8);
        config.setMaxThreads(20);
        config.setTaskTimeout(600); // 10 minutes
        config.setMaxRetries(5);
        return config;
    }
    
    public static TaskEngineConfig lightweightConfig() {
        TaskEngineConfig config = new TaskEngineConfig();
        config.setCoreThreads(1);
        config.setMaxThreads(3);
        config.setTaskTimeout(60); // 1 minute
        config.setMaxRetries(1);
        config.setEnableDistributedExecution(false);
        return config;
    }
    
    @Override
    public String toString() {
        return String.format("TaskEngineConfig{threads=%d/%d, timeout=%ds, retries=%d, distributed=%b, scheduling=%b}",
            coreThreads, maxThreads, taskTimeout, maxRetries, enableDistributedExecution, enableScheduling);
    }
}