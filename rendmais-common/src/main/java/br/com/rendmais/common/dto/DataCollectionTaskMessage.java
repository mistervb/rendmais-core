package br.com.rendmais.common.dto;

import lombok.*;

import java.util.Map;
import java.util.Set;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataCollectionTaskMessage {
    
    private String taskId;
    private String taskType; // "PRICE_COLLECTION", "PRODUCT_INFO", "MARKET_RESEARCH", "WEB_SCRAPING"
    private String targetUrl;
    private String dataType; // "prices", "product_info", "market_research", "reviews"
    
    // Task constraints for passive income
    private boolean requiresWifi;
    private long maxDataUsageMB;
    private int maxDurationMinutes;
    private int requestDelayMs;
    private boolean respectRobotsTxt;
    private String userAgent;
    
    // Data collection parameters
    private Map<String, String> collectionParams; // e.g., {"css_selector": ".price", "max_pages": "5"}
    private Set<String> requiredFields; // fields to extract
    private String dataFormat; // "json", "csv", "xml"
    
    // Task metadata
    private long createdTimestamp;
    private String requesterNodeId;
    private int priority; // 1-10, lower = higher priority for passive income
    private int maxRetries;
    
    // Progress tracking
    private TaskStatus status;
    private String assignedNodeId;
    private long startedTimestamp;
    private long completedTimestamp;
    private long dataCollectedMB;
    private int progressPercentage;
    private String errorMessage;
    
    public enum TaskStatus {
        PENDING,
        ASSIGNED,
        IN_PROGRESS,
        COMPLETED,
        FAILED,
        CANCELLED,
        TIMEOUT
    }
    
    public boolean isValidForPassiveIncome() {
        // Validate task is suitable for passive income execution
        if (requiresWifi && maxDataUsageMB > 50) {
            return false; // Too much data for WiFi-only tasks
        }
        
        if (maxDurationMinutes > 30) {
            return false; // Too long for passive income
        }
        
        if (requestDelayMs < 1000) {
            return false; // Too aggressive for passive income
        }
        
        if (targetUrl == null || targetUrl.isEmpty()) {
            return false;
        }
        
        if (!isWhitelistedUrl(targetUrl)) {
            return false;
        }
        
        return true;
    }
    
    private boolean isWhitelistedUrl(String url) {
        // Simple whitelist for passive income - only safe, legitimate websites
        String[] whitelistedDomains = {
            "amazon.com", "ebay.com", "walmart.com", "target.com", "bestbuy.com",
            "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
            "reddit.com", "twitter.com", "facebook.com", "linkedin.com",
            "wikipedia.org", "imdb.com", "rottentomatoes.com",
            "cnet.com", "techcrunch.com", "verge.com"
        };
        
        String lowerUrl = url.toLowerCase();
        for (String domain : whitelistedDomains) {
            if (lowerUrl.contains(domain)) {
                return true;
            }
        }
        return false;
    }
    
    public long getEstimatedCompletionTime() {
        // Simple estimation based on data size and complexity
        long baseTime = maxDataUsageMB * 2; // 2 minutes per MB
        long complexityFactor = collectionParams != null ? collectionParams.size() * 30 : 0; // 30 seconds per parameter
        return baseTime + complexityFactor;
    }
    
    public void updateProgress(int percentage, long dataCollectedMB) {
        this.progressPercentage = percentage;
        this.dataCollectedMB = dataCollectedMB;
        
        if (percentage >= 100) {
            this.status = TaskStatus.COMPLETED;
            this.completedTimestamp = System.currentTimeMillis();
        }
    }
    
    public void markFailed(String errorMessage) {
        this.status = TaskStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completedTimestamp = System.currentTimeMillis();
    }
    
    public void markAssigned(String nodeId) {
        this.status = TaskStatus.ASSIGNED;
        this.assignedNodeId = nodeId;
        this.startedTimestamp = System.currentTimeMillis();
    }
}