package br.com.rendmais.task.engine.plugin;

import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Passive Income Data Collection Plugin
 * Focuses on WiFi-based data collection to avoid CPU stress
 * Collects market intelligence data for passive monetization
 */
public class PassiveIncomeDataCollectionPlugin implements TaskPlugin {
    
    private static final Logger log = LoggerFactory.getLogger(PassiveIncomeDataCollectionPlugin.class);
    
    private static final String PLUGIN_ID = "passive-income-data-plugin";
    private static final String PLUGIN_NAME = "Passive Income Data Collection Plugin";
    private static final String PLUGIN_VERSION = "1.0.0";
    
    private static final int MAX_DATA_SIZE = 1024 * 1024; // 1MB max data size
    private static final int REQUEST_TIMEOUT = 30000; // 30 seconds timeout
    private static final int DELAY_BETWEEN_REQUESTS = 2000; // 2 seconds delay
    
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
        return Set.of("PRICE_COLLECTION", "PRODUCT_INFO", "MARKET_RESEARCH", "WEB_SCRAPING");
    }
    
    @Override
    public TaskResult execute(Task task) throws Exception {
        log.info("Executing {} task: {} for URL: {}", task.getTaskType(), task.getTaskId(), task.getTargetUrl());
        
        try {
            // Validate network conditions before execution
            if (!isWifiConnected()) {
                log.warn("Task {} skipped - WiFi not available", task.getTaskId());
                return TaskResult.failure(task.getTaskId(), "WiFi not available", "node-1");
            }
            
            if (!hasSufficientBattery()) {
                log.warn("Task {} skipped - Insufficient battery", task.getTaskId());
                return TaskResult.failure(task.getTaskId(), "Insufficient battery level", "node-1");
            }
            
            if (isDataLimitExceeded()) {
                log.warn("Task {} skipped - Daily data limit exceeded", task.getTaskId());
                return TaskResult.failure(task.getTaskId(), "Daily data limit exceeded", "node-1");
            }
            
            String result = collectData(task);
            
            log.info("Task {} completed successfully - collected {} bytes", task.getTaskId(), result.length());
            return TaskResult.success(task.getTaskId(), result, "node-1");
            
        } catch (Exception e) {
            log.error("Task {} failed with error", task.getTaskId(), e);
            return TaskResult.failure(task.getTaskId(), "Data collection failed: " + e.getMessage(), "node-1");
        }
    }
    
    private String collectData(Task task) throws Exception {
        String targetUrl = task.getTargetUrl();
        String collectionPattern = task.getCollectionPattern();
        String dataType = task.getDataType();
        
        log.debug("Collecting {} data from {} using pattern: {}", dataType, targetUrl, collectionPattern);
        
        // Add delay to avoid overwhelming servers
        Thread.sleep(DELAY_BETWEEN_REQUESTS);
        
        // Fetch webpage content
        String content = fetchWebpage(targetUrl);
        
        // Extract relevant data using pattern
        String extractedData = extractData(content, collectionPattern);
        
        // Validate data size
        if (extractedData.getBytes(StandardCharsets.UTF_8).length > MAX_DATA_SIZE) {
            log.warn("Extracted data exceeds maximum size, truncating");
            extractedData = truncateData(extractedData, MAX_DATA_SIZE);
        }
        
        // Format result with metadata
        return formatResult(extractedData, targetUrl, dataType, LocalDateTime.now());
    }
    
    private String fetchWebpage(String urlString) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        
        // Set user agent and headers
        connection.setRequestProperty("User-Agent", getUserAgent());
        connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        connection.setRequestProperty("Accept-Encoding", "gzip, deflate");
        connection.setRequestProperty("Connection", "keep-alive");
        
        connection.setConnectTimeout(REQUEST_TIMEOUT);
        connection.setReadTimeout(REQUEST_TIMEOUT);
        
        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new Exception("HTTP request failed with code: " + responseCode);
        }
        
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
                
                // Limit content size to prevent memory issues
                if (content.length() > MAX_DATA_SIZE * 2) {
                    log.warn("Webpage content exceeds safe size limit, truncating");
                    break;
                }
            }
        }
        
        connection.disconnect();
        return content.toString();
    }
    
    private String extractData(String content, String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return content; // Return full content if no pattern specified
        }
        
        try {
            Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            Matcher matcher = regex.matcher(content);
            
            StringBuilder extracted = new StringBuilder();
            int matchCount = 0;
            
            while (matcher.find() && matchCount < 100) { // Limit matches to prevent excessive data
                extracted.append(matcher.group()).append("\n");
                matchCount++;
            }
            
            if (extracted.length() == 0) {
                log.warn("No data extracted using pattern: {}", pattern);
                return "No matching data found";
            }
            
            return extracted.toString();
            
        } catch (Exception e) {
            log.error("Error extracting data with pattern: {}", pattern, e);
            return "Error extracting data: " + e.getMessage();
        }
    }
    
    private String formatResult(String data, String url, String dataType, LocalDateTime timestamp) {
        Map<String, Object> result = new HashMap<>();
        result.put("url", url);
        result.put("dataType", dataType);
        result.put("timestamp", timestamp.toString());
        result.put("dataSize", data.length());
        result.put("collectedData", data);
        result.put("pluginVersion", PLUGIN_VERSION);
        
        return result.toString();
    }
    
    private String truncateData(String data, int maxSize) {
        if (data.getBytes(StandardCharsets.UTF_8).length <= maxSize) {
            return data;
        }
        
        // Simple truncation - in production, might want smarter truncation
        return data.substring(0, Math.min(data.length(), maxSize / 2));
    }
    
    // Network and battery simulation methods (would be implemented with real APIs in production)
    private boolean isWifiConnected() {
        // Simulate WiFi check - in production, would use actual network APIs
        return true; // Assume WiFi is connected for now
    }
    
    private boolean hasSufficientBattery() {
        // Simulate battery check - in production, would use actual battery APIs
        return true; // Assume sufficient battery for now
    }
    
    private boolean isDataLimitExceeded() {
        // Simulate data usage check - in production, would track actual usage
        return false; // Assume data limit not exceeded for now
    }
    
    private String getUserAgent() {
        return config != null ? config.getOrDefault("userAgent", "RendMais-DataCollector/1.0") 
                              : "RendMais-DataCollector/1.0";
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
        if (task == null) {
            throw new IllegalArgumentException("Task cannot be null");
        }
        
        if (task.getTargetUrl() == null || task.getTargetUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("Target URL is required for data collection tasks");
        }
        
        if (!task.getTargetUrl().startsWith("http://") && !task.getTargetUrl().startsWith("https://")) {
            throw new IllegalArgumentException("Invalid URL format: " + task.getTargetUrl());
        }
        
        // Validate data type
        if (task.getDataType() == null || task.getDataType().trim().isEmpty()) {
            throw new IllegalArgumentException("Data type is required");
        }
        
        // Validate max requests
        if (task.getMaxRequests() <= 0 || task.getMaxRequests() > 50) {
            throw new IllegalArgumentException("Max requests must be between 1 and 50");
        }
        
        // Validate requires WiFi
        if (!task.isRequiresWifi()) {
            log.warn("Task {} does not require WiFi - this may consume mobile data", task.getTaskId());
        }
    }
    
    @Override
    public Map<String, Object> getPluginInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("description", "Passive income data collection plugin for market intelligence");
        info.put("supportedTypes", getSupportedTaskTypes());
        info.put("maxDataSize", MAX_DATA_SIZE);
        info.put("requestTimeout", REQUEST_TIMEOUT);
        info.put("requestDelay", DELAY_BETWEEN_REQUESTS);
        info.put("healthCheck", isHealthy);
        info.put("config", config != null ? config : new HashMap<>());
        return info;
    }
}