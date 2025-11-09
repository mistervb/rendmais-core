# RendMais P2P Module

A robust peer-to-peer networking module for distributed systems, providing secure peer discovery, connection management, message routing, and heartbeat monitoring.

## Features

- Secure peer authentication: handshake with digital signatures and challenge-response
- Automatic peer discovery: topology propagation with flood prevention
- Connection management: reconnection with exponential backoff and immediate retry on channel inactivity
- Heartbeat monitoring: health checking with timeout detection and cleanup
- Message routing: type-based routing with interceptors and error handling
- Multi-peer support: multiple concurrent connections per node
- Connection pooling: efficient lifecycle management and duplicate-connection prevention
- Unique node identity: each node has a unique cryptographic identity
- NodeId-based addressing: records `nodeId -> InetSocketAddress` to ensure reliable reconnection and message delivery

## Architecture

### Core Components

- P2PNodeBootstrap: main entry point that wires all P2P components and provides high-level APIs
- P2PServer: Netty-based server handling inbound connections and messages
- P2PClient: outbound connections with reconnection and address recording by nodeId
- ConnectionPool: connection lifecycle, states, timeouts, and reconnection strategy
- PeerRegistry: peer storage, states, and accurate address updates after handshake
- MessageRouter: type-based routing with interceptors and context
- PeerDiscoveryService: periodic advertisements, topology propagation, flood prevention
- HeartbeatManager: periodic heartbeats, timeout detection, and peer removal

### Message Types

- `HANDSHAKE`, `HANDSHAKE_RESPONSE`
- `HEARTBEAT`, `HEARTBEAT_RESPONSE`
- `PEER_ADVERTISEMENT`, `PEER_DISCOVERY`
- `CONNECTION_CLOSE`, `ERROR`
- `TASK_REQUEST`, `TASK_RESULT`

### Security

- Digital signatures for handshake and message integrity
- Challenge-response in handshake
- Public key authentication based on local wallet/identity

### Connection Management

- Automatic reconnection with exponential backoff
- Immediate reconnect attempt on `channelInactive` using recorded nodeId addresses
- Accurate address updates after handshake via `PeerRegistry.updatePeerAddress`
- Robust duplicate-connection prevention via `updateConnectionId`

## Integration Guide

### Maven Dependency

Add the module to any project that needs P2P capabilities:

```xml
<dependency>
  <groupId>br.com.rendmais</groupId>
  <artifactId>rendmais-p2p</artifactId>
  <version>${project.version}</version>
</dependency>
```

### Quick Start (Plain Java)

```java
// Start a P2P node on port 8080
P2PNodeBootstrap node = new P2PNodeBootstrap(8080);
node.start();

// Connect to a known peer (seed)
node.connectTo("localhost", 8081);

// Send a message to a specific peer
SignedMessage msg = SignedMessage.builder()
    .type(MessageType.TASK_REQUEST)
    .payload("{\"task\":\"compute\"}")
    .signature("sig")
    .build();
node.sendMessage("peer-node-id", msg);

// Broadcast
node.broadcastMessage(msg);

// Shutdown
node.stop();
```

### Integration with `rendmais-node` (Spring Boot)

- Initialize `P2PNodeBootstrap` in a Spring-managed bean at application startup.
- Provide seed peers via configuration (e.g., `application.yml` or environment variables).
- Expose `MessageRouter` to register custom handlers (e.g., task-related messages).
- Manage lifecycle on `ContextClosedEvent` to call `node.stop()`.

### Recommended Lifecycle

- Start: `node.start()`
- Connect seeds: `node.connectTo(host, port)` or `node.connectTo(PeerInfo)`
- Operate: `sendMessage`, `broadcastMessage`, `isConnectedTo`, `getConnectedPeerCount`
- Stop: `node.stop()` to shut down server, heartbeat, discovery, and connection pool

## Configuration

### System Properties

```properties
# Connection settings
p2p.connection.timeout=30000
p2p.connection.max-retries=5
p2p.connection.retry-delay=1000

# Heartbeat settings
p2p.heartbeat.interval=5000
p2p.heartbeat.timeout=15000

# Discovery settings
p2p.discovery.interval=10000
p2p.discovery.ttl=300000
p2p.discovery.max-advertisements=1000

# Thread pool settings
p2p.thread.pool.size=10
```

### Programmatic Configuration

```java
// Heartbeat
HeartbeatManager heartbeatManager = new HeartbeatManager(
    peerRegistry,
    connectionPool,
    router,
    5000,
    15000
);

// Discovery
PeerDiscoveryService discoveryService = new PeerDiscoveryService(
    peerRegistry,
    localNodeId,
    advertisementSender,
    10000,
    300000
);
```

## Testing

Run integration tests:

```bash
mvn -pl rendmais-p2p -am test
```

### Test Coverage

- Basic peer connections
- Peer discovery
- Heartbeat functionality
- Message broadcasting
- Connection recovery and reconnection
- Multi-peer scenarios

## Monitoring and Troubleshooting

- Logging: SLF4J; enable debug via `logback.xml` with `br.com.rendmais.p2p` logger
- Metrics (logged): connection times, routing latency, discovery success rate, heartbeat response, pool utilization
- Common issues:
  - Connection failures: firewall/port availability
  - Handshake failures: key configuration/signature verification
  - Discovery issues: network connectivity/intervals
  - Heartbeat timeouts: adjust timeout for network latency

## Dependencies

- Netty (networking)
- Gson (JSON)
- SLF4J (logging)
- BouncyCastle (crypto via rendmais-common)

## Production Checklist

- Unique node identity per instance (keys and nodeId) configured
- Seed peers provided and reachable
- Proper logging configuration (file rotation, levels)
- Resource limits tuned (`thread.pool.size`, heartbeat/discovery intervals)
- Firewall rules opened for server port
- Graceful shutdown wired to application lifecycle
- Monitoring hooks: collect logs/metrics for long-running stability
- Backoff and retry limits reviewed for deployment environment

## Recent Updates & Fixes

- Node identity fix: unique key pair and nodeId per node
- Reconnection reliability: nodeId-based address recording and immediate retry on `channelInactive`
- PeerRegistry address accuracy: addresses updated post-handshake
- ConnectionPool client reference: settable and used for reconnection
- Duplicate connection prevention: improved `updateConnectionId()` logic

## Future Enhancements

- NAT traversal support
- Advanced routing algorithms
- Load balancing across peers
- End-to-end encryption for all message types
- Peer reputation system
- Integration with external service discovery