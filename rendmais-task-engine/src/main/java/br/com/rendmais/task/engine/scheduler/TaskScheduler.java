package br.com.rendmais.task.engine.scheduler;

import br.com.rendmais.task.engine.executor.TaskExecutor;
import br.com.rendmais.task.engine.model.Task;
import br.com.rendmais.task.engine.model.TaskPriority;
import br.com.rendmais.task.engine.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskScheduler {
    
    private static final Logger log = LoggerFactory.getLogger(TaskScheduler.class);
    
    private final TaskExecutor taskExecutor;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    
    public TaskScheduler(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        this.scheduler = Executors.newScheduledThreadPool(5, new SchedulerThreadFactory());
    }
    
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            log.info("TaskScheduler started");
        }
    }
    
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Stopping TaskScheduler...");
            
            // Cancel all scheduled tasks
            scheduledTasks.values().forEach(future -> future.cancel(false));
            scheduledTasks.clear();
            
            // Shutdown scheduler
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            log.info("TaskScheduler stopped");
        }
    }
    
    public String scheduleTask(Task task, long delay, TimeUnit unit) {
        if (!isRunning.get()) {
            throw new IllegalStateException("TaskScheduler is not running");
        }
        
        String scheduleId = generateScheduleId(task);
        
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            try {
                log.debug("Executing scheduled task: {} (type: {})", task.getTaskId(), task.getTaskType());
                taskExecutor.submitTask(task);
            } catch (Exception e) {
                log.error("Failed to execute scheduled task: {}", task.getTaskId(), e);
            }
        }, delay, unit);
        
        scheduledTasks.put(scheduleId, future);
        log.info("Scheduled task: {} to run in {} {}", task.getTaskId(), delay, unit);
        
        return scheduleId;
    }
    
    public String scheduleTaskAtFixedRate(Task task, long initialDelay, long period, TimeUnit unit) {
        if (!isRunning.get()) {
            throw new IllegalStateException("TaskScheduler is not running");
        }
        
        String scheduleId = generateScheduleId(task) + "-fixed-rate";
        
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                Task recurringTask = createRecurringTask(task);
                log.debug("Executing recurring task: {} (type: {})", recurringTask.getTaskId(), recurringTask.getTaskType());
                taskExecutor.submitTask(recurringTask);
            } catch (Exception e) {
                log.error("Failed to execute recurring task: {}", task.getTaskId(), e);
            }
        }, initialDelay, period, unit);
        
        scheduledTasks.put(scheduleId, future);
        log.info("Scheduled recurring task: {} at fixed rate: {} {}", task.getTaskId(), period, unit);
        
        return scheduleId;
    }
    
    public String scheduleTaskWithFixedDelay(Task task, long initialDelay, long delay, TimeUnit unit) {
        if (!isRunning.get()) {
            throw new IllegalStateException("TaskScheduler is not running");
        }
        
        String scheduleId = generateScheduleId(task) + "-fixed-delay";
        
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
            try {
                Task recurringTask = createRecurringTask(task);
                log.debug("Executing recurring task: {} (type: {})", recurringTask.getTaskId(), recurringTask.getTaskType());
                taskExecutor.submitTask(recurringTask);
            } catch (Exception e) {
                log.error("Failed to execute recurring task: {}", task.getTaskId(), e);
            }
        }, initialDelay, delay, unit);
        
        scheduledTasks.put(scheduleId, future);
        log.info("Scheduled recurring task: {} with fixed delay: {} {}", task.getTaskId(), delay, unit);
        
        return scheduleId;
    }
    
    public boolean cancelScheduledTask(String scheduleId) {
        ScheduledFuture<?> future = scheduledTasks.remove(scheduleId);
        if (future != null) {
            boolean cancelled = future.cancel(false);
            if (cancelled) {
                log.info("Cancelled scheduled task: {}", scheduleId);
            }
            return cancelled;
        }
        return false;
    }
    
    public boolean isTaskScheduled(String scheduleId) {
        return scheduledTasks.containsKey(scheduleId);
    }
    
    public int getScheduledTaskCount() {
        return scheduledTasks.size();
    }
    
    public boolean isRunning() {
        return isRunning.get();
    }
    
    private String generateScheduleId(Task task) {
        return task.getTaskId() + "-" + System.currentTimeMillis();
    }
    
    private Task createRecurringTask(Task originalTask) {
        Task recurringTask = new Task();
        recurringTask.setTaskId(originalTask.getTaskId() + "-" + System.nanoTime());
        recurringTask.setTaskType(originalTask.getTaskType());
        recurringTask.setPluginId(originalTask.getPluginId());
        recurringTask.setPriority(originalTask.getPriority());
        recurringTask.setPayload(originalTask.getPayload());
        recurringTask.setMetadata(originalTask.getMetadata());
        recurringTask.setNodeId(originalTask.getNodeId());
        recurringTask.setCreatedAt(java.time.Instant.now());
        recurringTask.setStatus(TaskStatus.PENDING);
        return recurringTask;
    }
    
    private static class SchedulerThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "TaskScheduler-";
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}