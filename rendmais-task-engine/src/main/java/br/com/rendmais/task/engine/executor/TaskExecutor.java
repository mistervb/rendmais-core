package br.com.rendmais.task.engine.executor;

import br.com.rendmais.task.engine.exception.TaskException;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskPriority;
import br.com.rendmais.task.engine.model.TaskResult;
import br.com.rendmais.task.engine.model.TaskStatus;
import br.com.rendmais.task.engine.plugin.TaskPlugin;
import br.com.rendmais.task.engine.registry.TaskRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor {
    
    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);
    
    private final TaskRegistry taskRegistry;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> workQueue;
    private final int maxThreads;
    private final long taskTimeout;
    private final TimeUnit timeoutUnit;
    
    private volatile boolean isRunning = false;
    
    public TaskExecutor(TaskRegistry taskRegistry, int coreThreads, int maxThreads, 
                       long taskTimeout, TimeUnit timeoutUnit) {
        this.taskRegistry = taskRegistry;
        this.maxThreads = maxThreads;
        this.taskTimeout = taskTimeout;
        this.timeoutUnit = timeoutUnit;
        
        // Create priority-based work queue
        this.workQueue = new PriorityBlockingQueue<>(1000, new TaskPriorityComparator());
        
        // Create thread factory with custom naming
        ThreadFactory threadFactory = new TaskThreadFactory();
        
        // Create thread pool executor
        this.executor = new ThreadPoolExecutor(
            coreThreads,
            maxThreads,
            60L, TimeUnit.SECONDS,
            workQueue,
            threadFactory,
            new ThreadPoolExecutor.AbortPolicy()
        );
        
        this.isRunning = true;
        log.info("TaskExecutor initialized with coreThreads={}, maxThreads={}, timeout={} {}",
            coreThreads, maxThreads, taskTimeout, timeoutUnit);
    }
    
    public CompletableFuture<TaskResult> submitTask(Task task) {
        if (!isRunning) {
            return CompletableFuture.failedFuture(
                new TaskException("TaskExecutor is not running"));
        }
        
        // Update task status to pending
        task.setStatus(TaskStatus.PENDING);
        task.setScheduledAt(System.currentTimeMillis());
        
        // Create task wrapper
        TaskWrapper taskWrapper = new TaskWrapper(task);
        
        // Submit to executor
        return CompletableFuture.supplyAsync(() -> taskWrapper.call(), executor);
    }
    
    public void shutdown() {
        log.info("Shutting down TaskExecutor...");
        isRunning = false;
        
        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Forcing executor shutdown...");
                executor.shutdownNow();
            }
            log.info("TaskExecutor shutdown completed");
        } catch (InterruptedException e) {
            log.error("Interrupted during shutdown", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    public boolean isRunning() {
        return isRunning && !executor.isShutdown();
    }
    
    public int getActiveCount() {
        return executor.getActiveCount();
    }
    
    public int getQueueSize() {
        return executor.getQueue().size();
    }
    
    public int getCompletedTaskCount() {
        return (int) executor.getCompletedTaskCount();
    }
    
    public ExecutorStats getStats() {
        return new ExecutorStats(
            executor.getActiveCount(),
            executor.getQueue().size(),
            (int) executor.getCompletedTaskCount(),
            executor.getCorePoolSize(),
            executor.getMaximumPoolSize(),
            executor.getPoolSize()
        );
    }
    
    private class TaskWrapper implements Callable<TaskResult> {
        private final Task task;
        
        TaskWrapper(Task task) {
            this.task = task;
        }
        
        @Override
        public TaskResult call() {
            long startTime = System.currentTimeMillis();
            String threadName = Thread.currentThread().getName();
            
            log.debug("Executing task {} (type: {}) on thread {}", 
                task.getId(), task.getType(), threadName);
            
            try {
                // Update task status to running
                task.setStatus(TaskStatus.RUNNING);
                task.setStartedAt(startTime);
                
                // Get plugin for task type
                TaskPlugin plugin = taskRegistry.getPluginForTaskType(task.getType());
                if (plugin == null) {
                    throw new TaskException("No plugin found for task type: " + task.getType());
                }
                
                // Validate task
                if (!plugin.validateTask(task)) {
                    throw new TaskException("Task validation failed for task: " + task.getId());
                }
                
                // Execute task with timeout
                TaskResult result = executeWithTimeout(plugin, task);
                
                // Update task with result
                task.setCompletedAt(System.currentTimeMillis());
                task.setResult(result);
                
                if (result.isSuccess()) {
                    task.setStatus(TaskStatus.COMPLETED);
                    log.info("Task {} completed successfully in {}ms", 
                        task.getId(), System.currentTimeMillis() - startTime);
                } else {
                    task.setStatus(TaskStatus.FAILED);
                    task.setErrorMessage(result.getErrorMessage());
                    log.warn("Task {} failed: {}", task.getId(), result.getErrorMessage());
                }
                
                return result;
                
            } catch (TimeoutException e) {
                task.setStatus(TaskStatus.TIMEOUT);
                task.setErrorMessage("Task execution timed out");
                task.setCompletedAt(System.currentTimeMillis());
                log.error("Task {} timed out after {} {}", 
                    task.getId(), taskTimeout, timeoutUnit);
                
                return TaskResult.failure(task.getId(), "Task execution timed out", e);
                
            } catch (Exception e) {
                task.setStatus(TaskStatus.FAILED);
                task.setErrorMessage(e.getMessage());
                task.setCompletedAt(System.currentTimeMillis());
                log.error("Task {} failed with exception", task.getId(), e);
                
                return TaskResult.failure(task.getId(), e.getMessage(), e);
            }
        }
        
        private TaskResult executeWithTimeout(TaskPlugin plugin, Task task) throws Exception {
            ExecutorService timeoutExecutor = Executors.newSingleThreadExecutor();
            try {
                Future<TaskResult> future = timeoutExecutor.submit(() -> plugin.execute(task));
                return future.get(taskTimeout, timeoutUnit);
            } finally {
                timeoutExecutor.shutdownNow();
            }
        }
    }
    
    private static class TaskPriorityComparator implements Comparator<Runnable> {
        @Override
        public int compare(Runnable r1, Runnable r2) {
            if (r1 instanceof TaskWrapper && r2 instanceof TaskWrapper) {
                Task t1 = ((TaskWrapper) r1).task;
                Task t2 = ((TaskWrapper) r2).task;
                
                // Higher priority first
                int priorityCompare = Integer.compare(
                    t2.getPriority().getLevel(), 
                    t1.getPriority().getLevel()
                );
                
                if (priorityCompare != 0) {
                    return priorityCompare;
                }
                
                // Earlier scheduled tasks first
                return Long.compare(t1.getScheduledAt(), t2.getScheduledAt());
            }
            return 0;
        }
    }
    
    private static class TaskThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "TaskExecutor-";
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
    
    public static class ExecutorStats {
        private final int activeCount;
        private final int queueSize;
        private final int completedCount;
        private final int corePoolSize;
        private final int maxPoolSize;
        private final int poolSize;
        
        public ExecutorStats(int activeCount, int queueSize, int completedCount,
                           int corePoolSize, int maxPoolSize, int poolSize) {
            this.activeCount = activeCount;
            this.queueSize = queueSize;
            this.completedCount = completedCount;
            this.corePoolSize = corePoolSize;
            this.maxPoolSize = maxPoolSize;
            this.poolSize = poolSize;
        }
        
        public int getActiveCount() { return activeCount; }
        public int getQueueSize() { return queueSize; }
        public int getCompletedCount() { return completedCount; }
        public int getCorePoolSize() { return corePoolSize; }
        public int getMaxPoolSize() { return maxPoolSize; }
        public int getPoolSize() { return poolSize; }
        
        @Override
        public String toString() {
            return String.format("ExecutorStats{active=%d, queue=%d, completed=%d, pool=%d/%d/%d}",
                activeCount, queueSize, completedCount, corePoolSize, poolSize, maxPoolSize);
        }
    }
}