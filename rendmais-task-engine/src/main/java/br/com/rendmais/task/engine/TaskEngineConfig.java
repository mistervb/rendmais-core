package br.com.rendmais.task.engine;

import java.util.HashMap;
import java.util.Map;

public class TaskEngineConfig {
    
    // WiFi-based passive income configuration
    private int coreThreads = 1; // Reduced to minimize CPU usage
    private int maxThreads = 2; // Limited for passive operation
    private long taskTimeout = 1800; // 30 minutes for data collection tasks
    private int maxRetries = 2; // Reduced retries to save resources
    private boolean enableDistributedExecution = true;
    private boolean enableScheduling = true;
    
    // Passive income specific settings
    private boolean wifiOnly = true; // Only operate on WiFi networks
    private long minBatteryLevel = 20; // Minimum 20% battery required
    private long maxDailyDataUsage = 100; // 100MB daily limit
    private long requestDelay = 2000; // 2 second delay between requests
    private boolean respectRobotsTxt = true; // Always respect robots.txt
    private String userAgent = "RendMais-DataCollector/1.0";
    
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
    
    // Passive income specific getters and setters
    public boolean isWifiOnly() {
        return wifiOnly;
    }
    
    public void setWifiOnly(boolean wifiOnly) {
        this.wifiOnly = wifiOnly;
    }
    
    public long getMinBatteryLevel() {
        return minBatteryLevel;
    }
    
    public void setMinBatteryLevel(long minBatteryLevel) {
        this.minBatteryLevel = minBatteryLevel;
    }
    
    public long getMaxDailyDataUsage() {
        return maxDailyDataUsage;
    }
    
    public void setMaxDailyDataUsage(long maxDailyDataUsage) {
        this.maxDailyDataUsage = maxDailyDataUsage;
    }
    
    public long getRequestDelay() {
        return requestDelay;
    }
    
    public void setRequestDelay(long requestDelay) {
        this.requestDelay = requestDelay;
    }
    
    public boolean isRespectRobotsTxt() {
        return respectRobotsTxt;
    }
    
    public void setRespectRobotsTxt(boolean respectRobotsTxt) {
        this.respectRobotsTxt = respectRobotsTxt;
    }
    
    public String getUserAgent() {
        return userAgent;
    }
    
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
    
    public static TaskEngineConfig defaultConfig() {
        return new TaskEngineConfig();
    }
    
    public static TaskEngineConfig passiveIncomeConfig() {
        TaskEngineConfig config = new TaskEngineConfig();
        config.setCoreThreads(1);
        config.setMaxThreads(2);
        config.setTaskTimeout(1800); // 30 minutes for data collection
        config.setMaxRetries(2);
        config.setWifiOnly(true);
        config.setMinBatteryLevel(20);
        config.setMaxDailyDataUsage(100); // 100MB daily limit
        config.setRequestDelay(2000); // 2 second delay
        config.setRespectRobotsTxt(true);
        config.setUserAgent("RendMais-DataCollector/1.0");
        return config;
    }
    
    public static TaskEngineConfig lightweightConfig() {
        TaskEngineConfig config = new TaskEngineConfig();
        config.setCoreThreads(1);
        config.setMaxThreads(2);
        config.setTaskTimeout(900); // 15 minutes
        config.setMaxRetries(1);
        config.setEnableDistributedExecution(false);
        config.setWifiOnly(true);
        config.setMinBatteryLevel(30);
        config.setMaxDailyDataUsage(50); // 50MB daily limit
        config.setRequestDelay(3000); // 3 second delay
        return config;
    }
    
    @Override
    public String toString() {
        return String.format("TaskEngineConfig{threads=%d/%d, timeout=%ds, retries=%d, wifi=%b, battery=%d%%, data=%dMB, delay=%dms}",
            coreThreads, maxThreads, taskTimeout, maxRetries, wifiOnly, minBatteryLevel, maxDailyDataUsage, requestDelay);
    }
}