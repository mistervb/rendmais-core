package br.com.rendmais.common.dto;

import lombok.*;

import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(callSuper = true)
public class PassiveIncomePeerInfo extends PeerInfo {
    
    // Resource capabilities for passive income
    private boolean wifiOnly;
    private int maxConcurrentTasks;
    private long maxDailyDataUsageMB;
    private long currentDataUsageMB;
    private int batteryLevel;
    private boolean charging;
    private Set<String> supportedDataTypes; // e.g., "prices", "product_info", "market_research"
    private double averageTaskCompletionTime;
    private int completedTasksCount;
    private boolean acceptsDataCollectionTasks;
    private double reputationScore; // 0.0 to 1.0
    private long lastActivityTimestamp;
    private String userAgent;
    private boolean respectsRobotsTxt;
    private int requestDelayMs;
    
    // Network preferences
    private boolean prefersLowBandwidthTasks;
    private int maxTaskTimeoutMinutes;
    private Set<String> supportedWebsites; // websites this peer can access
    
    // Custom builder that extends PeerInfo builder
    public static PassiveIncomePeerInfoBuilder passiveBuilder() {
        return new PassiveIncomePeerInfoBuilder();
    }
    
    public static class PassiveIncomePeerInfoBuilder {
        private String nodeId;
        private String address;
        private int port;
        private String publicKeyBase64;
        
        private boolean wifiOnly;
        private int maxConcurrentTasks;
        private long maxDailyDataUsageMB;
        private long currentDataUsageMB;
        private int batteryLevel;
        private boolean charging;
        private Set<String> supportedDataTypes;
        private double averageTaskCompletionTime;
        private int completedTasksCount;
        private boolean acceptsDataCollectionTasks;
        private double reputationScore;
        private long lastActivityTimestamp;
        private String userAgent;
        private boolean respectsRobotsTxt;
        private int requestDelayMs;
        private boolean prefersLowBandwidthTasks;
        private int maxTaskTimeoutMinutes;
        private Set<String> supportedWebsites;
        
        public PassiveIncomePeerInfoBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder address(String address) {
            this.address = address;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder port(int port) {
            this.port = port;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder publicKeyBase64(String publicKeyBase64) {
            this.publicKeyBase64 = publicKeyBase64;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder wifiOnly(boolean wifiOnly) {
            this.wifiOnly = wifiOnly;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder maxConcurrentTasks(int maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder maxDailyDataUsageMB(long maxDailyDataUsageMB) {
            this.maxDailyDataUsageMB = maxDailyDataUsageMB;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder currentDataUsageMB(long currentDataUsageMB) {
            this.currentDataUsageMB = currentDataUsageMB;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder batteryLevel(int batteryLevel) {
            this.batteryLevel = batteryLevel;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder charging(boolean charging) {
            this.charging = charging;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder supportedDataTypes(Set<String> supportedDataTypes) {
            this.supportedDataTypes = supportedDataTypes;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder averageTaskCompletionTime(double averageTaskCompletionTime) {
            this.averageTaskCompletionTime = averageTaskCompletionTime;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder completedTasksCount(int completedTasksCount) {
            this.completedTasksCount = completedTasksCount;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder acceptsDataCollectionTasks(boolean acceptsDataCollectionTasks) {
            this.acceptsDataCollectionTasks = acceptsDataCollectionTasks;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder reputationScore(double reputationScore) {
            this.reputationScore = reputationScore;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder lastActivityTimestamp(long lastActivityTimestamp) {
            this.lastActivityTimestamp = lastActivityTimestamp;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder respectsRobotsTxt(boolean respectsRobotsTxt) {
            this.respectsRobotsTxt = respectsRobotsTxt;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder requestDelayMs(int requestDelayMs) {
            this.requestDelayMs = requestDelayMs;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder prefersLowBandwidthTasks(boolean prefersLowBandwidthTasks) {
            this.prefersLowBandwidthTasks = prefersLowBandwidthTasks;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder maxTaskTimeoutMinutes(int maxTaskTimeoutMinutes) {
            this.maxTaskTimeoutMinutes = maxTaskTimeoutMinutes;
            return this;
        }
        
        public PassiveIncomePeerInfoBuilder supportedWebsites(Set<String> supportedWebsites) {
            this.supportedWebsites = supportedWebsites;
            return this;
        }
        
        public PassiveIncomePeerInfo build() {
            PassiveIncomePeerInfo info = new PassiveIncomePeerInfo();
            // Set PeerInfo fields
            info.setNodeId(nodeId);
            info.setAddress(address);
            info.setPort(port);
            info.setPublicKeyBase64(publicKeyBase64);
            
            // Set PassiveIncomePeerInfo fields
            info.setWifiOnly(wifiOnly);
            info.setMaxConcurrentTasks(maxConcurrentTasks);
            info.setMaxDailyDataUsageMB(maxDailyDataUsageMB);
            info.setCurrentDataUsageMB(currentDataUsageMB);
            info.setBatteryLevel(batteryLevel);
            info.setCharging(charging);
            info.setSupportedDataTypes(supportedDataTypes);
            info.setAverageTaskCompletionTime(averageTaskCompletionTime);
            info.setCompletedTasksCount(completedTasksCount);
            info.setAcceptsDataCollectionTasks(acceptsDataCollectionTasks);
            info.setReputationScore(reputationScore);
            info.setLastActivityTimestamp(lastActivityTimestamp);
            info.setUserAgent(userAgent);
            info.setRespectsRobotsTxt(respectsRobotsTxt);
            info.setRequestDelayMs(requestDelayMs);
            info.setPrefersLowBandwidthTasks(prefersLowBandwidthTasks);
            info.setMaxTaskTimeoutMinutes(maxTaskTimeoutMinutes);
            info.setSupportedWebsites(supportedWebsites);
            
            return info;
        }
    }
    
    public boolean canAcceptTask(String dataType, long estimatedDataSizeMB, int estimatedDurationMinutes) {
        if (!acceptsDataCollectionTasks) {
            return false;
        }
        
        if (wifiOnly && !isOnWifi()) {
            return false;
        }
        
        if (currentDataUsageMB + estimatedDataSizeMB > maxDailyDataUsageMB) {
            return false;
        }
        
        if (batteryLevel < 20 && !charging) {
            return false;
        }
        
        if (supportedDataTypes != null && dataType != null && !dataType.isBlank() && !supportedDataTypes.contains(dataType)) {
            return false;
        }
        
        if (estimatedDurationMinutes > maxTaskTimeoutMinutes) {
            return false;
        }
        
        return true;
    }
    
    public boolean isWifiOnly() {
        return wifiOnly;
    }
    
    public void setWifiOnly(boolean wifiOnly) {
        this.wifiOnly = wifiOnly;
    }
    
    public void setNodeId(String nodeId) {
        super.setNodeId(nodeId);
    }
    
    public void setAddress(String address) {
        super.setAddress(address);
    }
    
    public void setPort(int port) {
        super.setPort(port);
    }
    
    public void setPublicKeyBase64(String publicKeyBase64) {
        super.setPublicKeyBase64(publicKeyBase64);
    }

    // Explicitly expose base PeerInfo getters to avoid annotation processing issues in downstream modules
    public String getNodeId() {
        return super.getNodeId();
    }
    public String getAddress() {
        return super.getAddress();
    }
    public int getPort() {
        return super.getPort();
    }
    public String getPublicKeyBase64() {
        return super.getPublicKeyBase64();
    }
    
    public boolean isOnWifi() {
        // This would be determined by actual network detection
        // For now, return true if wifiOnly is false (meaning peer can use any network)
        return !wifiOnly || true; // Simplified - in real implementation would check actual network
    }
    
    public void incrementTaskCount() {
        completedTasksCount++;
        lastActivityTimestamp = System.currentTimeMillis();
    }
    
    public void addDataUsage(long dataSizeMB) {
        this.currentDataUsageMB += dataSizeMB;
    }
    
    public double getReputationScore() {
        // Calculate reputation based on completion rate, speed, and reliability
        double completionRate = Math.min(1.0, completedTasksCount / 100.0); // Normalize to 1.0
        double speedScore = Math.min(1.0, 10.0 / (averageTaskCompletionTime + 1)); // Faster = better
        
        return (completionRate * 0.6) + (speedScore * 0.4); // Weighted average
    }
}