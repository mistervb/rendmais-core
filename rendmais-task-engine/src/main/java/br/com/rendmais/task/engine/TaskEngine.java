package br.com.rendmais.task.engine;

import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.task.engine.exception.TaskException;
import br.com.rendmais.task.engine.executor.TaskExecutor;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskPriority;
import br.com.rendmais.task.engine.model.TaskResult;
import br.com.rendmais.task.engine.model.TaskStatus;
import br.com.rendmais.task.engine.p2p.TaskP2PIntegration;
import br.com.rendmais.task.engine.plugin.TaskPlugin;
import br.com.rendmais.task.engine.registry.TaskRegistry;
import br.com.rendmais.task.engine.scheduler.TaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskEngine {
    
    private static final Logger log = LoggerFactory.getLogger(TaskEngine.class);
    
    private final TaskRegistry taskRegistry;
    private final TaskExecutor taskExecutor;
    private final TaskScheduler taskScheduler;
    private final TaskP2PIntegration p2pIntegration;
    private final Map<String, Task> activeTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    
    private final String nodeId;
    private final TaskEngineConfig config;
    
    public TaskEngine(String nodeId, TaskEngineConfig config, MessageRouter messageRouter) {
        this.nodeId = nodeId;
        this.config = config;
        
        // Initialize components
        this.taskRegistry = new TaskRegistry();
        this.taskExecutor = new TaskExecutor(
            taskRegistry,
            config.getCoreThreads(),
            config.getMaxThreads(),
            config.getTaskTimeout(),
            TimeUnit.SECONDS
        );
        this.taskScheduler = new TaskScheduler(taskExecutor);
        this.p2pIntegration = new TaskP2PIntegration(messageRouter, taskExecutor);
        
        log.info("TaskEngine initialized for node: {}", nodeId);
    }
    
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Starting TaskEngine...");
            
            // Start scheduler
            taskScheduler.start();
            
            // Initialize registered plugins
            for (String pluginId : taskRegistry.getRegisteredPluginIds()) {
                try {
                    taskRegistry.initializePlugin(pluginId, config.getPluginConfig(pluginId));
                } catch (Exception e) {
                    log.error("Failed to initialize plugin: {}", pluginId, e);
                }
            }
            
            log.info("TaskEngine started successfully");
        }
    }
    
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Stopping TaskEngine...");
            
            // Stop scheduler
            taskScheduler.stop();
            
            // Shutdown executor
            taskExecutor.shutdown();
            
            // Shutdown all plugins
            for (String pluginId : taskRegistry.getRegisteredPluginIds()) {
                try {
                    taskRegistry.unregisterPlugin(pluginId);
                } catch (Exception e) {
                    log.error("Failed to unregister plugin: {}", pluginId, e);
                }
            }
            
            log.info("TaskEngine stopped");
        }
    }
    
    public CompletableFuture<TaskResult> submitTask(Task task) {
        if (!isRunning.get()) {
            return CompletableFuture.failedFuture(
                new TaskException("TaskEngine is not running"));
        }
        
        // Set node ID
        task.setNodeId(nodeId);
        
        // Store active task
        activeTasks.put(task.getTaskId(), task);
        
        log.info("Submitting task: {} (type: {})", task.getTaskId(), task.getTaskType());
        
        // Submit to executor
        CompletableFuture<TaskResult> future = taskExecutor.submitTask(task);
        
        // Remove from active tasks when completed
        future.whenComplete((result, throwable) -> {
            activeTasks.remove(task.getTaskId());
            if (throwable != null) {
                log.error("Task {} failed", task.getTaskId(), throwable);
            }
        });
        
        return future;
    }
    
    public Task createTask(String taskType, String payload, Map<String, String> metadata) {
        return createTask(taskType, payload, metadata, TaskPriority.NORMAL);
    }
    
    public Task createTask(String taskType, String payload, Map<String, String> metadata, TaskPriority priority) {
        Task task = new Task();
        task.setTaskId(java.util.UUID.randomUUID().toString());
        task.setTaskType(taskType);
        task.setPayload(payload);
        task.setMetadata(metadata);
        task.setPriority(priority);
        task.setNodeId(nodeId);
        task.setStatus(TaskStatus.PENDING);
        return task;
    }
    
    public String scheduleTask(Task task, long delay, TimeUnit unit) {
        if (!isRunning.get()) {
            throw new IllegalStateException("TaskEngine is not running");
        }
        
        task.setNodeId(nodeId);
        return taskScheduler.scheduleTask(task, delay, unit);
    }
    
    public String scheduleRecurringTask(Task task, long initialDelay, long period, TimeUnit unit) {
        if (!isRunning.get()) {
            throw new IllegalStateException("TaskEngine is not running");
        }
        
        task.setNodeId(nodeId);
        return taskScheduler.scheduleTaskAtFixedRate(task, initialDelay, period, unit);
    }
    
    public void registerPlugin(TaskPlugin plugin) throws Exception {
        taskRegistry.registerPlugin(plugin);
        
        // Initialize plugin if engine is running
        if (isRunning.get()) {
            taskRegistry.initializePlugin(plugin.getPluginId(), config.getPluginConfig(plugin.getPluginId()));
        }
        
        log.info("Registered plugin: {} - {} v{}", 
            plugin.getPluginId(), plugin.getPluginName(), plugin.getPluginVersion());
    }
    
    public void unregisterPlugin(String pluginId) throws Exception {
        taskRegistry.unregisterPlugin(pluginId);
        log.info("Unregistered plugin: {}", pluginId);
    }
    
    public boolean cancelTask(String taskId) {
        Task task = activeTasks.get(taskId);
        if (task != null) {
            task.setStatus(TaskStatus.CANCELLED);
            activeTasks.remove(taskId);
            log.info("Cancelled task: {}", taskId);
            return true;
        }
        return false;
    }
    
    public Task getTask(String taskId) {
        return activeTasks.get(taskId);
    }
    
    public Map<String, Task> getActiveTasks() {
        return new ConcurrentHashMap<>(activeTasks);
    }
    
    public TaskRegistry getTaskRegistry() {
        return taskRegistry;
    }
    
    public TaskExecutor getTaskExecutor() {
        return taskExecutor;
    }
    
    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }
    
    public TaskP2PIntegration getP2PIntegration() {
        return p2pIntegration;
    }
    
    public boolean isRunning() {
        return isRunning.get();
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public TaskEngineConfig getConfig() {
        return config;
    }
    
    public TaskEngineStats getStats() {
        TaskExecutor.ExecutorStats executorStats = taskExecutor.getStats();
        return new TaskEngineStats(
            activeTasks.size(),
            taskScheduler.getScheduledTaskCount(),
            taskRegistry.getRegisteredPluginIds().size(),
            executorStats.getActiveCount(),
            executorStats.getQueueSize(),
            executorStats.getCompletedCount(),
            isRunning.get()
        );
    }
    
    public static class TaskEngineStats {
        private final int activeTasks;
        private final int scheduledTasks;
        private final int registeredPlugins;
        private final int executorActive;
        private final int executorQueue;
        private final int executorCompleted;
        private final boolean isRunning;
        
        public TaskEngineStats(int activeTasks, int scheduledTasks, int registeredPlugins,
                             int executorActive, int executorQueue, int executorCompleted,
                             boolean isRunning) {
            this.activeTasks = activeTasks;
            this.scheduledTasks = scheduledTasks;
            this.registeredPlugins = registeredPlugins;
            this.executorActive = executorActive;
            this.executorQueue = executorQueue;
            this.executorCompleted = executorCompleted;
            this.isRunning = isRunning;
        }
        
        public int getActiveTasks() { return activeTasks; }
        public int getScheduledTasks() { return scheduledTasks; }
        public int getRegisteredPlugins() { return registeredPlugins; }
        public int getExecutorActive() { return executorActive; }
        public int getExecutorQueue() { return executorQueue; }
        public int getExecutorCompleted() { return executorCompleted; }
        public boolean isRunning() { return isRunning; }
        
        @Override
        public String toString() {
            return String.format("TaskEngineStats{active=%d, scheduled=%d, plugins=%d, executor={active=%d, queue=%d, completed=%d}, running=%b}",
                activeTasks, scheduledTasks, registeredPlugins, executorActive, executorQueue, executorCompleted, isRunning);
        }
    }
}