package br.com.rendmais.p2p;

import br.com.rendmais.common.dto.SignedMessage;
import br.com.rendmais.common.enums.MessageType;
import br.com.rendmais.p2p.registry.PeerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the P2P module.
 * Tests the complete P2P node lifecycle including connection management,
 * peer discovery, heartbeat system, and message routing.
 */
public class P2PIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(P2PIntegrationTest.class);
    
    private P2PNodeBootstrap node1;
    private P2PNodeBootstrap node2;
    private P2PNodeBootstrap node3;
    
    private static final int NODE1_PORT = 8081;
    private static final int NODE2_PORT = 8082;
    private static final int NODE3_PORT = 8083;
    
    @BeforeEach
    public void setUp() throws Exception {
        log.info("Setting up P2P integration test");
        
        // Create three nodes
        node1 = new P2PNodeBootstrap(NODE1_PORT);
        node2 = new P2PNodeBootstrap(NODE2_PORT);
        node3 = new P2PNodeBootstrap(NODE3_PORT);
        
        // Start all nodes
        node1.start();
        node2.start();
        node3.start();
        
        // Wait for nodes to initialize
        Thread.sleep(2000);
        
        log.info("All nodes started successfully");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        log.info("Tearing down P2P integration test");
        
        // Stop all nodes
        if (node3 != null) node3.stop();
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
        
        // Wait for cleanup
        Thread.sleep(1000);
        
        log.info("All nodes stopped");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testBasicPeerConnection() throws Exception {
        log.info("Testing basic peer connection");
        
        // Get node2 ID before connecting
        String node2Id = node2.getLocalNodeId();
        
        // Connect node1 to node2
        node1.connectTo("localhost", NODE2_PORT);
        
        // Wait for connection and handshake
        Thread.sleep(5000);
        
        // Verify connection
        assertTrue(node1.isConnectedTo(node2Id),
                "Node1 should be connected to Node2");
        
        log.info("Basic peer connection test passed");
    }
    
    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    public void testPeerDiscovery() throws Exception {
        log.info("Testing peer discovery");
        
        // Get node IDs
        String node2Id = node2.getLocalNodeId();
        String node3Id = node3.getLocalNodeId();
        
        // Connect node1 to node2
        node1.connectTo("localhost", NODE2_PORT);
        Thread.sleep(5000);
        
        // Connect node2 to node3 (node1 should discover node3 through node2)
        node2.connectTo("localhost", NODE3_PORT);
        Thread.sleep(12000); // Increased wait time for discovery to propagate
        
        // Verify that node1 discovered node3
        PeerRegistry node1Registry = node1.getRegistry();
        assertTrue(node1Registry.isPeerConnected(node3Id), "Node1 should have discovered Node3");
        
        log.info("Peer discovery test passed");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testHeartbeatSystem() throws Exception {
        log.info("Testing heartbeat system");
        
        // Get node2 ID
        String node2Id = node2.getLocalNodeId();
        
        // Connect node1 to node2
        node1.connectTo("localhost", NODE2_PORT);
        Thread.sleep(5000);
        
        // Wait for heartbeats to be exchanged
        Thread.sleep(10000);
        
        // Verify that peers are still connected (heartbeat kept them alive)
        assertTrue(node1.isConnectedTo(node2Id), "Node1 should still be connected to Node2 after heartbeats");
        
        log.info("Heartbeat system test passed");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMessageBroadcast() throws Exception {
        log.info("Testing message broadcast");
        
        // Get node IDs
        String node2Id = node2.getLocalNodeId();
        String node3Id = node3.getLocalNodeId();
        
        // Connect node1 to node2 and node3
        node1.connectTo("localhost", NODE2_PORT);
        node1.connectTo("localhost", NODE3_PORT);
        Thread.sleep(5000);
        
        // Create a test message
        SignedMessage testMessage = SignedMessage.builder()
                .type(MessageType.TASK_REQUEST)
                .payload("{\"test\": \"broadcast message\"}")
                .signature("test-signature")
                .build();
        
        // Broadcast message from node1
        node1.broadcastMessage(testMessage);
        Thread.sleep(2000);
        
        // Verify that all nodes are still connected after broadcast
        assertTrue(node1.isConnectedTo(node2Id),
                "Node1 should be connected to Node2 after broadcast");
        assertTrue(node1.isConnectedTo(node3Id),
                "Node1 should be connected to Node3 after broadcast");
        
        log.info("Message broadcast test passed");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testConnectionRecovery() throws Exception {
        log.info("Testing connection recovery");
        
        // Get node2 ID
        String node2Id = node2.getLocalNodeId();
        
        // Connect node1 to node2
        node1.connectTo("localhost", NODE2_PORT);
        Thread.sleep(5000);
        
        assertTrue(node1.isConnectedTo(node2Id), "Initial connection should be established");
        
        // Simulate connection issues by stopping and restarting node2
        node2.stop();
        Thread.sleep(2000);
        
        // Restart node2 with same identity (same port should preserve identity)
        node2 = new P2PNodeBootstrap(NODE2_PORT);
        node2.start();
        Thread.sleep(2000);
        
        // Node1 should automatically reconnect to node2
        Thread.sleep(10000);
        
        // Verify reconnection - node2 should have same ID since it's using same port
        String newNode2Id = node2.getLocalNodeId();
        log.info("Original node2 ID: {}, New node2 ID: {}", node2Id, newNode2Id);
        assertEquals(node2Id, newNode2Id, "Node2 should maintain same identity after restart");
        assertTrue(node1.isConnectedTo(node2Id), "Node1 should have reconnected to Node2");
        
        log.info("Connection recovery test passed");
    }
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMultiplePeerConnections() throws Exception {
        log.info("Testing multiple peer connections");
        
        // Get node IDs
        String node2Id = node2.getLocalNodeId();
        String node3Id = node3.getLocalNodeId();
        
        // Connect all nodes to each other
        node1.connectTo("localhost", NODE2_PORT);
        node1.connectTo("localhost", NODE3_PORT);
        node2.connectTo("localhost", NODE3_PORT);
        
        Thread.sleep(8000);
        
        // Verify all connections
        assertTrue(node1.isConnectedTo(node2Id), "Node1 should be connected to Node2");
        assertTrue(node1.isConnectedTo(node3Id), "Node1 should be connected to Node3");
        assertTrue(node2.isConnectedTo(node3Id), "Node2 should be connected to Node3");
        
        // Debug: Print actual connection counts and details
        System.out.println("=== DEBUG: Connection Counts ===");
        System.out.println("Node1 ID: " + node1.getLocalNodeId());
        System.out.println("Node2 ID: " + node2.getLocalNodeId());
        System.out.println("Node3 ID: " + node3.getLocalNodeId());
        System.out.println("Node1 connected peers: " + node1.getConnectedPeerCount());
        System.out.println("Node2 connected peers: " + node2.getConnectedPeerCount());
        System.out.println("Node3 connected peers: " + node3.getConnectedPeerCount());
        System.out.println("Node1 is connected to node2: " + node1.isConnectedTo(node2Id));
        System.out.println("Node1 is connected to node3: " + node1.isConnectedTo(node3Id));
        System.out.println("Node2 is connected to node3: " + node2.isConnectedTo(node3Id));
        
        // Verify peer counts
        assertEquals(2, node1.getConnectedPeerCount(), "Node1 should have 2 connected peers");
        assertEquals(2, node2.getConnectedPeerCount(), "Node2 should have 2 connected peers");
        assertEquals(2, node3.getConnectedPeerCount(), "Node3 should have 2 connected peers");
        
        log.info("Multiple peer connections test passed");
    }
}