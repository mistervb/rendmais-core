package br.com.rendmais.task.engine;

import br.com.rendmais.p2p.messaging.MessageRouter;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskPriority;
import br.com.rendmais.task.engine.model.TaskResult;
import br.com.rendmais.task.engine.plugin.EchoTaskPlugin;
import br.com.rendmais.task.engine.plugin.MathTaskPlugin;
import br.com.rendmais.task.engine.exception.TaskException;
import br.com.rendmais.task.engine.executor.TaskExecutor;
import br.com.rendmais.task.engine.TaskEngineConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class TaskEngineTest {
    
    private TaskEngine taskEngine;
    private MessageRouter messageRouter;
    
    @BeforeEach
    void setUp() {
        messageRouter = new MessageRouter();
        TaskEngineConfig config = TaskEngineConfig.defaultConfig();
        config.setCoreThreads(2);
        config.setMaxThreads(5);
        config.setTaskTimeout(10);
        
        taskEngine = new TaskEngine("test-node", config, messageRouter);
        taskEngine.start();
    }
    
    @AfterEach
    void tearDown() {
        if (taskEngine != null) {
            taskEngine.stop();
        }
    }
    
    @Test
    void testTaskEngineLifecycle() {
        assertThat(taskEngine.isRunning()).isTrue();
        
        TaskEngine.TaskEngineStats stats = taskEngine.getStats();
        assertThat(stats.isRunning()).isTrue();
        assertThat(stats.getRegisteredPlugins()).isEqualTo(0);
    }
    
    @Test
    void testRegisterPlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        assertThat(taskEngine.getTaskRegistry().isPluginRegistered(plugin.getPluginId())).isTrue();
        assertThat(taskEngine.getTaskRegistry().getSupportedTaskTypes()).contains("ECHO");
    }
    
    @Test
    void testUnregisterPlugin() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        taskEngine.unregisterPlugin(plugin.getPluginId());
        
        assertThat(taskEngine.getTaskRegistry().isPluginRegistered(plugin.getPluginId())).isFalse();
    }
    
    @Test
    void testSubmitEchoTask() throws Exception {
        // Register plugin
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Create and submit task
        Task task = taskEngine.createTask("ECHO", "Hello World", Map.of("test", "true"));
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        // Wait for result
        TaskResult result = future.get(5, TimeUnit.SECONDS);
        
        assertThat(result.isSuccessful()).isTrue();
        assertThat(result.getResult()).isEqualTo("Echo: Hello World");
        assertThat(result.getTaskId()).isEqualTo(task.getTaskId());
    }
    
    @Test
    void testSubmitMathTask() throws Exception {
        // Register plugin
        MathTaskPlugin plugin = new MathTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Create and submit addition task
        Task task = taskEngine.createTask("ADD", "5,3,2", Map.of("test", "true"));
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        // Wait for result
        TaskResult result = future.get(5, TimeUnit.SECONDS);
        
        assertThat(result.isSuccessful()).isTrue();
        assertThat(result.getResult()).isEqualTo("10"); // 5 + 3 + 2
    }
    
    @Test
    void testTaskValidation() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Test invalid task (null payload)
        Task invalidTask = taskEngine.createTask("ECHO", null, Map.of());
        CompletableFuture<TaskResult> future = taskEngine.submitTask(invalidTask);
        
        TaskResult result = future.get(5, TimeUnit.SECONDS);
        assertThat(result.isSuccessful()).isFalse();
    }
    
    @Test
    void testTaskPriority() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        // Create tasks with different priorities
        Task highPriorityTask = taskEngine.createTask("ECHO", "High Priority", Map.of(), TaskPriority.HIGH);
        Task lowPriorityTask = taskEngine.createTask("ECHO", "Low Priority", Map.of(), TaskPriority.LOW);
        
        // Submit tasks
        CompletableFuture<TaskResult> highFuture = taskEngine.submitTask(highPriorityTask);
        CompletableFuture<TaskResult> lowFuture = taskEngine.submitTask(lowPriorityTask);
        
        // Both should complete successfully
        TaskResult highResult = highFuture.get(5, TimeUnit.SECONDS);
        TaskResult lowResult = lowFuture.get(5, TimeUnit.SECONDS);
        
        assertThat(highResult.isSuccessful()).isTrue();
        assertThat(lowResult.isSuccessful()).isTrue();
    }
    
    @Test
    void testScheduledTask() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        Task task = taskEngine.createTask("ECHO", "Scheduled Task", Map.of());
        
        // Schedule task to run in 1 second
        String scheduleId = taskEngine.scheduleTask(task, 1, TimeUnit.SECONDS);
        
        assertThat(scheduleId).isNotNull();
        
        // Wait for task to complete
        Thread.sleep(2000);
        
        // Task should be completed
        assertThat(taskEngine.getTask(task.getTaskId())).isNull(); // Task should be removed from active tasks
    }
    
    @Test
    void testTaskCancellation() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        Task task = taskEngine.createTask("ECHO", "Cancel Test", Map.of());
        
        // Submit task
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        // Cancel task
        boolean cancelled = taskEngine.cancelTask(task.getTaskId());
        
        assertThat(cancelled).isTrue();
        assertThat(taskEngine.getTask(task.getTaskId())).isNull();
    }
    
    @Test
    void testEngineStats() throws Exception {
        EchoTaskPlugin plugin = new EchoTaskPlugin();
        taskEngine.registerPlugin(plugin);
        
        TaskEngine.TaskEngineStats stats = taskEngine.getStats();
        
        assertThat(stats.isRunning()).isTrue();
        assertThat(stats.getRegisteredPlugins()).isEqualTo(1);
        assertThat(stats.getActiveTasks()).isEqualTo(0);
        
        // Submit a task and wait for completion
        Task task = taskEngine.createTask("ECHO", "Stats Test", Map.of());
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        // Wait for task to complete
        future.get(2, TimeUnit.SECONDS);
        
        // Stats should update - task should be completed and removed from active tasks
        stats = taskEngine.getStats();
        assertThat(stats.getActiveTasks()).isEqualTo(0); // Task should be completed and removed
    }
    
    @Test
    void testEngineNotRunning() {
        taskEngine.stop();
        
        Task task = taskEngine.createTask("ECHO", "Test", Map.of());
        
        CompletableFuture<TaskResult> future = taskEngine.submitTask(task);
        
        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(TaskException.class)
            .hasMessageContaining("TaskEngine is not running");
    }
}