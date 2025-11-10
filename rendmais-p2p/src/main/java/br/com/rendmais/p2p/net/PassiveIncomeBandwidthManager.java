package br.com.rendmais.p2p.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PassiveIncomeBandwidthManager {
    
    private static final Logger log = LoggerFactory.getLogger(PassiveIncomeBandwidthManager.class);
    
    // Default limits for passive income (conservative)
    private static final long DEFAULT_DAILY_DATA_LIMIT_MB = 100; // 100MB per day
    private static final long DEFAULT_HOURLY_DATA_LIMIT_MB = 20;   // 20MB per hour
    private static final long DEFAULT_TASK_DATA_LIMIT_MB = 5;      // 5MB per task
    private static final long DEFAULT_REQUEST_DELAY_MS = 2000;     // 2 seconds between requests
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 2; // Max 2 concurrent requests
    
    // Rate limiting
    private final long dailyDataLimitMB;
    private final long hourlyDataLimitMB;
    private final long taskDataLimitMB;
    private final long requestDelayMs;
    private final int maxConcurrentRequests;
    
    // Usage tracking
    private final AtomicLong dailyDataUsageMB = new AtomicLong(0);
    private final AtomicLong hourlyDataUsageMB = new AtomicLong(0);
    private final AtomicReference<LocalDate> currentDate = new AtomicReference<>(LocalDate.now());
    private final AtomicReference<LocalDateTime> currentHour = new AtomicReference<>(LocalDateTime.now().withMinute(0).withSecond(0).withNano(0));
    
    // Rate limiting
    private final Semaphore concurrentRequestSemaphore;
    private final Map<String, Long> lastRequestTime = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    
    // Robots.txt compliance
    private final RobotsTxtCompliance robotsCompliance;
    
    // WiFi-only mode
    private volatile boolean wifiOnly = true;
    private volatile boolean currentlyOnWifi = false;
    
    public PassiveIncomeBandwidthManager() {
        this(DEFAULT_DAILY_DATA_LIMIT_MB, DEFAULT_HOURLY_DATA_LIMIT_MB, 
             DEFAULT_TASK_DATA_LIMIT_MB, DEFAULT_REQUEST_DELAY_MS, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }
    
    public PassiveIncomeBandwidthManager(long dailyDataLimitMB, long hourlyDataLimitMB, 
                                       long taskDataLimitMB, long requestDelayMs, int maxConcurrentRequests) {
        this.dailyDataLimitMB = dailyDataLimitMB;
        this.hourlyDataLimitMB = hourlyDataLimitMB;
        this.taskDataLimitMB = taskDataLimitMB;
        this.requestDelayMs = requestDelayMs;
        this.maxConcurrentRequests = maxConcurrentRequests;
        
        this.concurrentRequestSemaphore = new Semaphore(maxConcurrentRequests, true);
        this.robotsCompliance = new RobotsTxtCompliance();
        
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "bandwidth-manager");
            t.setDaemon(true);
            return t;
        });
        
        startUsageMonitoring();
        log.info("Initialized PassiveIncomeBandwidthManager with daily limit: {}MB, hourly limit: {}MB, task limit: {}MB",
                dailyDataLimitMB, hourlyDataLimitMB, taskDataLimitMB);
    }
    
    public boolean canMakeRequest(String url, long estimatedDataSizeMB) {
        // Check WiFi-only mode
        if (wifiOnly && !currentlyOnWifi) {
            log.warn("Request blocked: WiFi-only mode enabled but not on WiFi");
            return false;
        }
        
        // Check robots.txt compliance
        if (!robotsCompliance.isAllowed(url)) {
            log.warn("Request blocked by robots.txt: {}", url);
            return false;
        }
        
        // Check data limits
        if (!checkDataLimits(estimatedDataSizeMB)) {
            log.warn("Request blocked by data limits: daily={}MB, hourly={}MB, task={}MB",
                    dailyDataUsageMB.get(), hourlyDataUsageMB.get(), estimatedDataSizeMB);
            return false;
        }
        
        // Check rate limiting
        if (!checkRateLimit(url)) {
            log.warn("Request blocked by rate limiting for URL: {}", url);
            return false;
        }
        
        return true;
    }
    
    public boolean acquireRequestSlot(long timeoutMs) throws InterruptedException {
        return concurrentRequestSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
    }
    
    public void releaseRequestSlot() {
        concurrentRequestSemaphore.release();
    }
    
    public void recordDataUsage(String url, long dataSizeMB) {
        dailyDataUsageMB.addAndGet(dataSizeMB);
        hourlyDataUsageMB.addAndGet(dataSizeMB);
        
        // Update last request time for rate limiting
        String domain = extractDomain(url);
        lastRequestTime.put(domain, System.currentTimeMillis());
        
        log.debug("Recorded data usage: {}MB for {} (daily: {}MB, hourly: {}MB)", 
                dataSizeMB, url, dailyDataUsageMB.get(), hourlyDataUsageMB.get());
    }
    
    private boolean checkDataLimits(long estimatedDataSizeMB) {
        // Reset daily usage if date changed
        LocalDate today = LocalDate.now();
        if (!currentDate.get().equals(today)) {
            currentDate.set(today);
            dailyDataUsageMB.set(0);
            log.info("Daily data usage reset for new date: {}", today);
        }
        
        // Reset hourly usage if hour changed
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime currentHourStart = now.withMinute(0).withSecond(0).withNano(0);
        if (currentHour.get().isBefore(currentHourStart)) {
            currentHour.set(currentHourStart);
            hourlyDataUsageMB.set(0);
            log.info("Hourly data usage reset for new hour: {}", currentHourStart);
        }
        
        // Check limits
        long projectedDailyUsage = dailyDataUsageMB.get() + estimatedDataSizeMB;
        long projectedHourlyUsage = hourlyDataUsageMB.get() + estimatedDataSizeMB;
        
        if (projectedDailyUsage > dailyDataLimitMB) {
            log.warn("Daily data limit exceeded: {}MB > {}MB", projectedDailyUsage, dailyDataLimitMB);
            return false;
        }
        
        if (projectedHourlyUsage > hourlyDataLimitMB) {
            log.warn("Hourly data limit exceeded: {}MB > {}MB", projectedHourlyUsage, hourlyDataLimitMB);
            return false;
        }
        
        if (estimatedDataSizeMB > taskDataLimitMB) {
            log.warn("Task data limit exceeded: {}MB > {}MB", estimatedDataSizeMB, taskDataLimitMB);
            return false;
        }
        
        return true;
    }
    
    private boolean checkRateLimit(String url) {
        String domain = extractDomain(url);
        Long lastTime = lastRequestTime.get(domain);
        
        if (lastTime == null) {
            return true;
        }
        
        long currentTime = System.currentTimeMillis();
        long timeSinceLastRequest = currentTime - lastTime;
        
        if (timeSinceLastRequest < requestDelayMs) {
            log.debug("Rate limit enforced for {}: {}ms < {}ms", domain, timeSinceLastRequest, requestDelayMs);
            return false;
        }
        
        return true;
    }
    
    private String extractDomain(String url) {
        try {
            // Simple domain extraction - in production, use proper URL parsing
            if (url.startsWith("http://")) {
                url = url.substring(7);
            } else if (url.startsWith("https://")) {
                url = url.substring(8);
            }
            
            int slashIndex = url.indexOf('/');
            if (slashIndex > 0) {
                url = url.substring(0, slashIndex);
            }
            
            return url;
        } catch (Exception e) {
            log.warn("Error extracting domain from URL: {}", url, e);
            return "unknown";
        }
    }
    
    private void startUsageMonitoring() {
        // Log usage statistics periodically
        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.info("Data usage statistics - Daily: {}MB/{}, Hourly: {}MB/{}, Concurrent requests: {}",
                        dailyDataUsageMB.get(), dailyDataLimitMB,
                        hourlyDataUsageMB.get(), hourlyDataLimitMB,
                        maxConcurrentRequests - concurrentRequestSemaphore.availablePermits());
            } catch (Exception e) {
                log.error("Error in usage monitoring", e);
            }
        }, 5, 5, TimeUnit.MINUTES);
        
        // Reset hourly usage every hour
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime currentHourStart = now.withMinute(0).withSecond(0).withNano(0);
                currentHour.set(currentHourStart);
                hourlyDataUsageMB.set(0);
                log.info("Hourly data usage reset automatically");
            } catch (Exception e) {
                log.error("Error in hourly reset", e);
            }
        }, 60, 60, TimeUnit.MINUTES);
    }
    
    // Configuration methods
    public void setWifiOnly(boolean wifiOnly) {
        this.wifiOnly = wifiOnly;
        log.info("WiFi-only mode set to: {}", wifiOnly);
    }
    
    public void setCurrentlyOnWifi(boolean onWifi) {
        this.currentlyOnWifi = onWifi;
        log.info("Currently on WiFi: {}", onWifi);
    }
    
    public void updateDataLimits(long dailyMB, long hourlyMB, long taskMB) {
        log.info("Updating data limits - Daily: {}MB, Hourly: {}MB, Task: {}MB", dailyMB, hourlyMB, taskMB);
        // Note: In a real implementation, these would be volatile fields
        // For now, we just log the change
    }
    
    public void updateRateLimit(long delayMs, int maxConcurrent) {
        log.info("Updating rate limits - Delay: {}ms, Max concurrent: {}", delayMs, maxConcurrent);
        // Note: In a real implementation, these would be volatile fields
        // For now, we just log the change
    }
    
    // Status methods
    public BandwidthStatus getCurrentStatus() {
        return new BandwidthStatus(
            dailyDataUsageMB.get(),
            dailyDataLimitMB,
            hourlyDataUsageMB.get(),
            hourlyDataLimitMB,
            maxConcurrentRequests - concurrentRequestSemaphore.availablePermits(),
            maxConcurrentRequests,
            wifiOnly,
            currentlyOnWifi
        );
    }
    
    public static class BandwidthStatus {
        private final long dailyUsageMB;
        private final long dailyLimitMB;
        private final long hourlyUsageMB;
        private final long hourlyLimitMB;
        private final int activeRequests;
        private final int maxRequests;
        private final boolean wifiOnly;
        private final boolean onWifi;
        
        public BandwidthStatus(long dailyUsageMB, long dailyLimitMB, long hourlyUsageMB, long hourlyLimitMB,
                             int activeRequests, int maxRequests, boolean wifiOnly, boolean onWifi) {
            this.dailyUsageMB = dailyUsageMB;
            this.dailyLimitMB = dailyLimitMB;
            this.hourlyUsageMB = hourlyUsageMB;
            this.hourlyLimitMB = hourlyLimitMB;
            this.activeRequests = activeRequests;
            this.maxRequests = maxRequests;
            this.wifiOnly = wifiOnly;
            this.onWifi = onWifi;
        }
        
        public boolean canAcceptNewTask(long estimatedDataSizeMB) {
            long projectedDaily = dailyUsageMB + estimatedDataSizeMB;
            long projectedHourly = hourlyUsageMB + estimatedDataSizeMB;
            
            return projectedDaily <= dailyLimitMB && 
                   projectedHourly <= hourlyLimitMB && 
                   activeRequests < maxRequests &&
                   (!wifiOnly || onWifi);
        }
        
        public double getDailyUsagePercentage() {
            return dailyLimitMB > 0 ? (double) dailyUsageMB / dailyLimitMB * 100 : 0;
        }
        
        public double getHourlyUsagePercentage() {
            return hourlyLimitMB > 0 ? (double) hourlyUsageMB / hourlyLimitMB * 100 : 0;
        }
        
        public String getStatusSummary() {
            return String.format("Daily: %dMB/%dMB (%.1f%%), Hourly: %dMB/%dMB (%.1f%%), Requests: %d/%d, WiFi: %s%s",
                    dailyUsageMB, dailyLimitMB, getDailyUsagePercentage(),
                    hourlyUsageMB, hourlyLimitMB, getHourlyUsagePercentage(),
                    activeRequests, maxRequests, onWifi ? "Yes" : "No",
                    wifiOnly ? " (Required)" : "");
        }
    }
    
    public void shutdown() {
        log.info("Shutting down PassiveIncomeBandwidthManager");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    // Robots.txt compliance helper class
    private static class RobotsTxtCompliance {
        private final Map<String, Boolean> allowedDomains = new ConcurrentHashMap<>();
        
        public boolean isAllowed(String url) {
            // Simple implementation - in production, this would fetch and parse robots.txt
            String domain = extractDomainFromUrl(url);
            return allowedDomains.computeIfAbsent(domain, k -> true); // Default to allowed
        }
        
        private String extractDomainFromUrl(String url) {
            try {
                if (url.startsWith("http://")) {
                    url = url.substring(7);
                } else if (url.startsWith("https://")) {
                    url = url.substring(8);
                }
                
                int slashIndex = url.indexOf('/');
                if (slashIndex > 0) {
                    url = url.substring(0, slashIndex);
                }
                
                return url;
            } catch (Exception e) {
                return "unknown";
            }
        }
    }
}