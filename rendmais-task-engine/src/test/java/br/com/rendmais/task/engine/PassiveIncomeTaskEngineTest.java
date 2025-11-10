package br.com.rendmais.task.engine;

import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskPriority;
import br.com.rendmais.task.engine.model.TaskResult;
import br.com.rendmais.task.engine.plugin.PassiveIncomeDataCollectionPlugin;
import br.com.rendmais.task.engine.exception.TaskException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class for passive income data collection functionality
 * Focuses on WiFi-based data collection with minimal CPU usage
 */
class PassiveIncomeTaskEngineTest {
    
    private TaskEngine taskEngine;
    private MessageRouter messageRouter;
    
    @BeforeEach
    void setUp() {
        messageRouter = new MessageRouter();
        // Use passive income configuration
        TaskEngineConfig config = TaskEngineConfig.passiveIncomeConfig();
        
        taskEngine = new TaskEngine("passive-income-node", config, messageRouter);
        taskEngine.start();
    }
    
    @AfterEach
    void tearDown() {
        if (taskEngine != null) {
            taskEngine.stop();
        }
    }
    
    @Test
    void testPassiveIncomeConfiguration() {
        TaskEngineConfig config = TaskEngineConfig.passiveIncomeConfig();
        
        // Verify passive income settings
        assertThat(config.getCoreThreads()).isEqualTo(1);
        assertThat(config.getMaxThreads()).isEqualTo(2);
        assertThat(config.getTaskTimeout()).isEqualTo(1800); // 30 minutes
        assertThat(config.getMaxRetries()).isEqualTo(2);
        assertThat(config.isWifiOnly()).isTrue();
        assertThat(config.getMinBatteryLevel()).isEqualTo(20);
        assertThat(config.getMaxDailyDataUsage()).isEqualTo(100); // 100MB
        assertThat(config.getRequestDelay()).isEqualTo(2000); // 2 seconds
        assertThat(config.isRespectRobotsTxt()).isTrue();
        assertThat(config.getUserAgent()).isEqualTo("RendMais-DataCollector/1.0");
    }
    
    @Test
    void testDataCollectionTaskCreation() {
        // Create data collection task
        Task task = Task.createDataCollectionTask(
            "PRICE_COLLECTION", 
            "passive-income-data-plugin",
            "https://example.com/products",
            "price",
            "<span class=\"price\">(\\d+\\.\\d+)</span>"
        );
        
        // Verify task properties
        assertThat(task.getTaskType()).isEqualTo("PRICE_COLLECTION");
        assertThat(task.getPluginId()).isEqualTo("passive-income-data-plugin");
        assertThat(task.getTargetUrl()).isEqualTo("https://example.com/products");
        assertThat(task.getDataType()).isEqualTo("price");
        assertThat(task.getCollectionPattern()).isEqualTo("<span class=\"price\">(\\d+\\.\\d+)</span>");
        assertThat(task.getPriority()).isEqualTo(TaskPriority.LOW);
        assertThat(task.isRequiresWifi()).isTrue();
        assertThat(task.getMaxRequests()).isEqualTo(10);
        assertThat(task.getMaxRetries()).isEqualTo(2);
        assertThat(task.getDataSize()).isEqualTo(1024 * 1024); // 1MB default
        assertThat(task.getEstimatedDuration()).isEqualTo(Duration.ofMinutes(5));
    }
    
    @Test
    void testPriceCollectionTask() {
        Task task = Task.createPriceCollectionTask(
            "passive-income-data-plugin",
            "https://example.com/products",
            "<span class=\"price\">(\\d+\\.\\d+)</span>"
        );
        
        assertThat(task.getTaskType()).isEqualTo("PRICE_COLLECTION");
        assertThat(task.getTargetUrl()).isEqualTo("https://example.com/products");
        assertThat(task.getDataType()).isEqualTo("price");
    }
    
    @Test
    void testProductInfoTask() {
        Task task = Task.createProductInfoTask(
            "passive-income-data-plugin",
            "https://example.com/products",
            "<h1 class=\"product-title\">(.*?)</h1>"
        );
        
        assertThat(task.getTaskType()).isEqualTo("PRODUCT_INFO");
        assertThat(task.getTargetUrl()).isEqualTo("https://example.com/products");
        assertThat(task.getDataType()).isEqualTo("product_info");
    }
    
    @Test
    void testMarketResearchTask() {
        Task task = Task.createMarketResearchTask(
            "passive-income-data-plugin",
            "https://example.com/market-data",
            "<div class=\"market-stats\">(.*?)</div>"
        );
        
        assertThat(task.getTaskType()).isEqualTo("MARKET_RESEARCH");
        assertThat(task.getTargetUrl()).isEqualTo("https://example.com/market-data");
        assertThat(task.getDataType()).isEqualTo("market_data");
    }
    
    @Test
    void testPassiveIncomeDataCollectionPluginRegistration() throws Exception {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Verify plugin registration
        assertThat(taskEngine.getTaskRegistry().isPluginRegistered(plugin.getPluginId())).isTrue();
        assertThat(taskEngine.getTaskRegistry().getSupportedTaskTypes())
            .contains("PRICE_COLLECTION", "PRODUCT_INFO", "MARKET_RESEARCH", "WEB_SCRAPING");
    }
    
    @Test
    void testTaskValidationForDataCollection() throws Exception {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Test valid data collection task
        Task validTask = Task.createPriceCollectionTask(
            "passive-income-data-plugin",
            "https://example.com/products",
            "<span class=\"price\">(\\d+\\.\\d+)</span>"
        );
        
        // Should not throw exception
        assertThatCode(() -> plugin.validateTask(validTask)).doesNotThrowAnyException();
    }
    
    @Test
    void testTaskValidationInvalidUrl() {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        
        Task invalidTask = Task.createDataCollectionTask(
            "PRICE_COLLECTION",
            "passive-income-data-plugin", 
            "invalid-url",
            "price",
            "pattern"
        );
        
        assertThatThrownBy(() -> plugin.validateTask(invalidTask))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid URL format");
    }
    
    @Test
    void testTaskValidationMissingDataType() {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        
        Task invalidTask = Task.builder()
            .taskId("test-task")
            .taskType("PRICE_COLLECTION")
            .pluginId("passive-income-data-plugin")
            .targetUrl("https://example.com")
            .dataType("") // Empty data type
            .build();
        
        assertThatThrownBy(() -> plugin.validateTask(invalidTask))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Data type is required");
    }
    
    @Test
    void testPassiveIncomeTaskScheduling() throws Exception {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Create a scheduled data collection task
        Task task = Task.createPriceCollectionTask(
            "passive-income-data-plugin",
            "https://example.com/products",
            "<span class=\"price\">(\\d+\\.\\d+)</span>"
        );
        
        // Schedule task to run in 1 second
        String scheduleId = taskEngine.scheduleTask(task, 1, TimeUnit.SECONDS);
        
        assertThat(scheduleId).isNotNull();
        
        // Wait for task to complete
        Thread.sleep(2000);
        
        // Task should be completed
        assertThat(taskEngine.getTask(task.getTaskId())).isNull(); // Task should be removed from active tasks
    }
    
    @Test
    void testPassiveIncomeEngineStats() throws Exception {
        PassiveIncomeDataCollectionPlugin plugin = new PassiveIncomeDataCollectionPlugin();
        taskEngine.registerPlugin(plugin);
        
        TaskEngine.TaskEngineStats stats = taskEngine.getStats();
        
        assertThat(stats.isRunning()).isTrue();
        assertThat(stats.getRegisteredPlugins()).isEqualTo(1);
        assertThat(stats.getActiveTasks()).isEqualTo(0);
        
        // Create a data collection task
        Task task = Task.createMarketResearchTask(
            "passive-income-data-plugin",
            "https://example.com/market-data",
            "<div class=\"market-stats\">(.*?)</div>"
        );
        
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        // Wait for task to complete
        future.get(10, TimeUnit.SECONDS);
        
        // Stats should update
        stats = taskEngine.getStats();
        assertThat(stats.getActiveTasks()).isEqualTo(0); // Task should be completed and removed
    }
    
    @Test
    void testLightweightConfiguration() {
        TaskEngineConfig lightweightConfig = TaskEngineConfig.lightweightConfig();
        
        assertThat(lightweightConfig.getCoreThreads()).isEqualTo(1);
        assertThat(lightweightConfig.getMaxThreads()).isEqualTo(2);
        assertThat(lightweightConfig.getTaskTimeout()).isEqualTo(900); // 15 minutes
        assertThat(lightweightConfig.getMaxRetries()).isEqualTo(1);
        assertThat(lightweightConfig.isWifiOnly()).isTrue();
        assertThat(lightweightConfig.getMinBatteryLevel()).isEqualTo(30);
        assertThat(lightweightConfig.getMaxDailyDataUsage()).isEqualTo(50); // 50MB
        assertThat(lightweightConfig.getRequestDelay()).isEqualTo(3000); // 3 seconds
    }
    
    @Test
    void testPassiveIncomeTaskWithMetadata() {
        Task task = Task.createDataCollectionTask(
            "MARKET_RESEARCH",
            "passive-income-data-plugin",
            "https://example.com/market-data",
            "market_data",
            "<div class=\"stats\">(.*?)</div>"
        );
        
        // Add metadata for passive income tracking
        task.setMetadata(Map.of(
            "market_segment", "electronics",
            "region", "north_america",
            "collection_frequency", "daily",
            "data_quality", "high"
        ));
        
        assertThat(task.getMetadata()).containsEntry("market_segment", "electronics");
        assertThat(task.getMetadata()).containsEntry("region", "north_america");
        assertThat(task.getMetadata()).containsEntry("collection_frequency", "daily");
        assertThat(task.getMetadata()).containsEntry("data_quality", "high");
    }
}