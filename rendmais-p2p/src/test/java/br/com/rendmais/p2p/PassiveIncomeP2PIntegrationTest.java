package br.com.rendmais.p2p;

import br.com.rendmais.common.dto.DataCollectionTaskMessage;
import br.com.rendmais.common.dto.PassiveIncomePeerInfo;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.net.PassiveIncomeBandwidthManager;
import com.google.gson.Gson;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PassiveIncomeP2PIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(PassiveIncomeP2PIntegrationTest.class);
    private static final int BASE_PORT = 9000;
    private static final Gson gson = new Gson();
    
    private PassiveIncomeP2PNodeBootstrap node1;
    private PassiveIncomeP2PNodeBootstrap node2;
    private PassiveIncomeP2PNodeBootstrap node3;
    
    @BeforeEach
    void setUp() throws InterruptedException {
        log.info("Setting up passive income P2P integration test");
        
        // Create three nodes with different configurations
        node1 = new PassiveIncomeP2PNodeBootstrap(BASE_PORT + 1, true, 100, 2); // WiFi-only, 100MB/day, 2 tasks
        node2 = new PassiveIncomeP2PNodeBootstrap(BASE_PORT + 2, false, 200, 3); // Any network, 200MB/day, 3 tasks  
        node3 = new PassiveIncomeP2PNodeBootstrap(BASE_PORT + 3, true, 50, 1);  // WiFi-only, 50MB/day, 1 task
        
        // Start nodes
        node1.start();
        node2.start();
        node3.start();
        
        // Give nodes time to start
        Thread.sleep(2000);
        
        // Connect nodes
        node1.connectTo("localhost", BASE_PORT + 2); // node1 -> node2
        node2.connectTo("localhost", BASE_PORT + 3); // node2 -> node3
        
        // Give connections time to establish
        Thread.sleep(3000);
    }
    
    @AfterEach
    void tearDown() {
        log.info("Tearing down passive income P2P integration test");
        
        if (node1 != null) {
            node1.stop();
        }
        if (node2 != null) {
            node2.stop();
        }
        if (node3 != null) {
            node3.stop();
        }
        
        // Give time for cleanup
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Test
    void testPassiveIncomePeerDiscovery() {
        log.info("Testing passive income peer discovery");
        
        // Verify that nodes can discover each other's passive income capabilities
        assertTrue(waitForPeerDiscovery(node1, node2.getLocalNodeId(), 5000));
        assertTrue(waitForPeerDiscovery(node2, node1.getLocalNodeId(), 5000));
        assertTrue(waitForPeerDiscovery(node2, node3.getLocalNodeId(), 5000));
        assertTrue(waitForPeerDiscovery(node3, node2.getLocalNodeId(), 5000));
        
        // Verify passive income peer info is available
        PassiveIncomePeerInfo node1Info = node1.getLocalPassiveIncomeInfo();
        PassiveIncomePeerInfo node2Info = node2.getLocalPassiveIncomeInfo();
        PassiveIncomePeerInfo node3Info = node3.getLocalPassiveIncomeInfo();
        
        assertNotNull(node1Info);
        assertNotNull(node2Info);
        assertNotNull(node3Info);
        
        // Verify configurations
        assertTrue(node1Info.isWifiOnly());
        assertFalse(node2Info.isWifiOnly());
        assertTrue(node3Info.isWifiOnly());
        
        assertEquals(100, node1Info.getMaxDailyDataUsageMB());
        assertEquals(200, node2Info.getMaxDailyDataUsageMB());
        assertEquals(50, node3Info.getMaxDailyDataUsageMB());
        
        assertEquals(2, node1Info.getMaxConcurrentTasks());
        assertEquals(3, node2Info.getMaxConcurrentTasks());
        assertEquals(1, node3Info.getMaxConcurrentTasks());
        
        log.info("Passive income peer discovery test completed successfully");
    }
    
    @Test
    void testDataCollectionTaskSubmission() throws Exception {
        log.info("Testing data collection task submission");
        
        // Create a data collection task
        DataCollectionTaskMessage task = createSampleTask("price_collection_1", "price", "https://amazon.com/api/prices");
        
        // Submit task from node1 (should be routed to suitable peer)
        CompletableFuture<DataCollectionTaskMessage> future = node1.submitDataCollectionTask(task);
        
        // Wait for task completion
        DataCollectionTaskMessage result = future.get(30, TimeUnit.SECONDS);
        
        assertNotNull(result);
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result.getStatus());
        assertEquals(100, result.getProgressPercentage());
        assertTrue(result.getDataCollectedMB() > 0);
        
        log.info("Data collection task submitted and completed successfully");
    }
    
    @Test
    void testBandwidthManagement() {
        log.info("Testing bandwidth management");
        
        // Check initial bandwidth status
        PassiveIncomeBandwidthManager.BandwidthStatus status1 = node1.getBandwidthStatus();
        PassiveIncomeBandwidthManager.BandwidthStatus status2 = node2.getBandwidthStatus();
        
        assertNotNull(status1);
        assertNotNull(status2);
        
        // Verify initial state
        assertTrue(status1.canAcceptNewTask(10)); // 10MB task should be acceptable
        assertTrue(status2.canAcceptNewTask(20)); // 20MB task should be acceptable
        
        // Test WiFi-only mode
        assertTrue(status1.getStatusSummary().contains("WiFi: Yes"));
        
        log.info("Bandwidth management test completed successfully");
    }
    
    @Test
    void testTaskRoutingBasedOnCapabilities() throws Exception {
        log.info("Testing task routing based on peer capabilities");
        
        // Create tasks that require WiFi (should prefer node1 and node3 over node2)
        DataCollectionTaskMessage wifiTask1 = createSampleTask("wifi_task_1", "price", "https://walmart.com/prices");
        wifiTask1.setRequiresWifi(true);
        wifiTask1.setMaxDataUsageMB(20);
        
        DataCollectionTaskMessage wifiTask2 = createSampleTask("wifi_task_2", "product", "https://target.com/products");
        wifiTask2.setRequiresWifi(true);
        wifiTask2.setMaxDataUsageMB(25);
        
        // Submit WiFi-only tasks
        CompletableFuture<DataCollectionTaskMessage> future1 = node1.submitDataCollectionTask(wifiTask1);
        CompletableFuture<DataCollectionTaskMessage> future2 = node1.submitDataCollectionTask(wifiTask2);
        
        // Wait for completion
        DataCollectionTaskMessage result1 = future1.get(30, TimeUnit.SECONDS);
        DataCollectionTaskMessage result2 = future2.get(30, TimeUnit.SECONDS);
        
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result1.getStatus());
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result2.getStatus());
        
        log.info("Task routing based on capabilities test completed successfully");
    }
    
    @Test
    void testConcurrentTaskLimits() throws Exception {
        log.info("Testing concurrent task limits");
        
        // Node3 has maxConcurrentTasks = 1, so let's test this limit
        
        // Create multiple tasks
        DataCollectionTaskMessage task1 = createSampleTask("concurrent_1", "market_research", "https://bestbuy.com/market1");
        DataCollectionTaskMessage task2 = createSampleTask("concurrent_2", "market_research", "https://google.com/market2");
        DataCollectionTaskMessage task3 = createSampleTask("concurrent_3", "market_research", "https://bing.com/market3");
        
        // Submit tasks to node3 (which has limit of 1 concurrent task)
        CompletableFuture<DataCollectionTaskMessage> future1 = node3.submitDataCollectionTask(task1);
        CompletableFuture<DataCollectionTaskMessage> future2 = node3.submitDataCollectionTask(task2);
        CompletableFuture<DataCollectionTaskMessage> future3 = node3.submitDataCollectionTask(task3);
        
        // Wait for results
        DataCollectionTaskMessage result1 = future1.get(30, TimeUnit.SECONDS);
        DataCollectionTaskMessage result2 = future2.get(30, TimeUnit.SECONDS);
        DataCollectionTaskMessage result3 = future3.get(30, TimeUnit.SECONDS);
        
        // All should complete (some might be routed to other peers due to capacity)
        assertNotNull(result1);
        assertNotNull(result2);
        assertNotNull(result3);
        
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result1.getStatus());
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result2.getStatus());
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result3.getStatus());
        
        log.info("Concurrent task limits test completed successfully");
    }
    
    @Test
    void testResourceUpdates() {
        log.info("Testing resource updates");
        
        // Update node1's resources
        node1.updateLocalResources(75, false, true); // 75% battery, not charging, on WiFi
        
        // Give time for resource update to propagate
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Verify the update was applied locally
        PassiveIncomePeerInfo updatedInfo = node1.getLocalPassiveIncomeInfo();
        assertEquals(75, updatedInfo.getBatteryLevel());
        assertFalse(updatedInfo.isCharging());
        assertTrue(updatedInfo.isOnWifi());
        
        log.info("Resource updates test completed successfully");
    }
    
    @Test
    void testTaskProgressTracking() throws Exception {
        log.info("Testing task progress tracking");
        
        // Create a task with progress tracking
        DataCollectionTaskMessage task = createSampleTask("progress_task", "price", "https://amazon.com/prices");
        task.setRequestDelayMs(1000); // Minimum delay for passive income validation
        
        // Submit task and track progress
        CompletableFuture<DataCollectionTaskMessage> future = node1.submitDataCollectionTask(task);
        
        // Wait for completion
        DataCollectionTaskMessage result = future.get(30, TimeUnit.SECONDS);
        
        assertNotNull(result);
        assertEquals(DataCollectionTaskMessage.TaskStatus.COMPLETED, result.getStatus());
        assertEquals(100, result.getProgressPercentage());
        
        log.info("Task progress tracking test completed successfully");
    }
    
    // Helper methods
    
    private DataCollectionTaskMessage createSampleTask(String taskId, String taskType, String targetUrl) {
        DataCollectionTaskMessage task = new DataCollectionTaskMessage();
        task.setTaskId(taskId);
        task.setTaskType(taskType);
        task.setTargetUrl(targetUrl);
        task.setRequiresWifi(false);
        task.setMaxDataUsageMB(10);
        task.setMaxDurationMinutes(15); // Add missing field - must be <= 30
        task.setRequestDelayMs(1000);
        task.setRespectRobotsTxt(true);
        task.setCollectionParams(java.util.Map.of("timeout", "30000"));
        task.setDataFormat("json");
        task.setPriority(5);
        task.setMaxRetries(3);
        task.setCreatedTimestamp(System.currentTimeMillis());
        task.setRequesterNodeId("test_node");
        return task;
    }
    
    private boolean waitForPeerDiscovery(PassiveIncomeP2PNodeBootstrap node, String peerNodeId, long timeoutMs) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeoutMs;
        
        String localNodeId = node.getLocalPassiveIncomeInfo().getNodeId();
        System.out.println("DEBUG: Waiting for peer discovery: " + localNodeId + " -> " + peerNodeId);
        
        while (System.currentTimeMillis() < endTime) {
            boolean connected = node.isConnectedTo(peerNodeId);
            PassiveIncomePeerInfo peerInfo = node.getPassiveIncomePeerInfo(peerNodeId);
            
            System.out.println("DEBUG: Discovery status: connected=" + connected + ", peerInfo=" + (peerInfo != null) + 
                             " (" + localNodeId + " -> " + peerNodeId + ")");
            
            // Check if node is connected AND has passive income peer info
            if (connected && peerInfo != null) {
                System.out.println("DEBUG: Peer discovery successful: " + localNodeId + " -> " + peerNodeId + 
                                 " (nodeId: " + peerInfo.getNodeId() + ")");
                return true;
            }
            
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        System.out.println("DEBUG: Peer discovery timeout: " + localNodeId + " -> " + peerNodeId);
        return false;
    }
}