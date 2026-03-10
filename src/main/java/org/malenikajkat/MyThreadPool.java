package org.malenikajkat;

import org.jetbrains.annotations.NotNull;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final List<BlockingQueue<Runnable>> queues;
    private final List<Worker> workers = Collections.synchronizedList(new ArrayList<>());
    private final ThreadFactory threadFactory;
    private volatile boolean isShutdown = false;
    private volatile boolean terminated = false;
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);
    private final RejectedExecutionHandler handler;
    private final Object terminationLock = new Object();
    private final int minSpareThreads;

    public MyThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit,
                        int queueSize, int minSpareThreads, RejectedExecutionHandler handler) {
        if (corePoolSize < 0) throw new IllegalArgumentException("corePoolSize < 0");
        if (maxPoolSize < 1) throw new IllegalArgumentException("maxPoolSize < 1");
        if (corePoolSize > maxPoolSize) throw new IllegalArgumentException("corePoolSize > maxPoolSize");
        if (minSpareThreads > maxPoolSize) throw new IllegalArgumentException("minSpareThreads > maxPoolSize");
        if (keepAliveTime < 0) throw new IllegalArgumentException("keepAliveTime < 0");

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = unit.toMillis(keepAliveTime);
        this.handler = handler != null ? handler : new CallerRunsPolicy();
        this.threadFactory = new MyThreadFactory("MyPool");
        this.queues = new ArrayList<>(maxPoolSize);
        for (int i = 0; i < maxPoolSize; i++) {
            queues.add(new LinkedBlockingQueue<>(queueSize));
        }
        this.minSpareThreads = minSpareThreads;
        startCoreWorkers();
    }

    private void startCoreWorkers() {
        synchronized (workers) {
            int targetSize = Math.min(Math.max(corePoolSize, minSpareThreads), maxPoolSize);
            while (workers.size() < targetSize) {
                addWorker();
            }
        }
    }

    private void addWorker() {
        synchronized (workers) {
            if (workers.size() >= maxPoolSize) return;
            int idx = workers.size();
            BlockingQueue<Runnable> queue = queues.get(idx);
            Worker worker = new Worker(queue);
            workers.add(worker);
            Thread t = threadFactory.newThread(worker);
            if (t == null) {
                workers.remove(worker);
                throw new IllegalStateException("ThreadFactory returned null thread");
            }
            t.start();
        }
    }

    @Override
    public void execute(@NotNull Runnable command) {
        if (isShutdown) {
            throw new RejectedExecutionException("Pool is shutdown");
        }

        boolean accepted = trySubmitToQueues(command);

        if (accepted) {
            return;
        }

        synchronized (workers) {
            if (workers.size() < maxPoolSize) {
                addWorker();
                if (trySubmitToQueues(command)) {
                    return;
                }
            }
        }

        handler.rejected(command, this);
    }

    private boolean trySubmitToQueues(Runnable command) {
        synchronized (workers) {
            if (workers.isEmpty()) return false;
            int startIndex = Math.floorMod(nextQueueIndex.getAndIncrement(), workers.size());
            for (int i = 0; i < workers.size(); i++) {
                int idx = (startIndex + i) % workers.size();
                if (queues.get(idx).offer(command)) {
                    LogHelper.info("[Pool] Task accepted into queue #{}: {}", idx, command);
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public <T> Future<T> submit(@NotNull Callable<T> callable) {
        CustomFutureTask<T> task = new CustomFutureTask<>(callable);
        try {
            execute(task);
        } catch (RejectedExecutionException e) {
            task.setExceptionPublic(e);
        }
        return task;
    }

    private static class CustomFutureTask<T> extends FutureTask<T> {
        public CustomFutureTask(@NotNull Callable<T> callable) {
            super(callable);
        }

        public void setExceptionPublic(Throwable t) {
            super.setException(t);
        }
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        synchronized (workers) {
            for (Worker w : workers) w.stop();
        }
        checkIfTerminated();
    }

    @Override
    @NotNull
    public List<Runnable> shutdownNow() {
        isShutdown = true;
        List<Runnable> drainedTasks = new ArrayList<>();
        synchronized (workers) {
            for (Worker w : workers) w.interrupt();
            queues.forEach(queue -> {
                List<Runnable> batch = new ArrayList<>();
                queue.drainTo(batch);
                drainedTasks.addAll(batch);
            });
        }
        checkIfTerminated();
        return drainedTasks;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanosTimeout = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanosTimeout;
        synchronized (terminationLock) {
            while (!terminated) {
                if (nanosTimeout <= 0) {
                    return false;
                }
                long millis = nanosTimeout / 1_000_000;
                int nanos = (int) (nanosTimeout % 1_000_000);
                terminationLock.wait(millis, nanos);
                nanosTimeout = deadline - System.nanoTime();
            }
            return true;
        }
    }

    private void checkIfTerminated() {
        synchronized (workers) {
            if (isShutdown && workers.isEmpty()) {
                synchronized (terminationLock) {
                    terminated = true;
                    terminationLock.notifyAll();
                }
            }
        }
    }

    @SuppressWarnings("unused")
    public int getActiveThreads() {
        synchronized (workers) {
            return (int) workers.stream().filter(w -> !w.isIdle()).count();
        }
    }

    @SuppressWarnings("unused")
    public int getTotalQueuedTasks() {
        return queues.stream().mapToInt(BlockingQueue::size).sum();
    }

    @SuppressWarnings("unused")
    public int getPoolSize() {
        synchronized (workers) {
            return workers.size();
        }
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private volatile boolean running = true;
        private volatile boolean idle = true;
        private Thread thread;

        Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        public boolean isIdle() { return idle; }
        public void stop() { running = false; }
        public void interrupt() {
            if (thread != null) {
                thread.interrupt();
                running = false;
            }
        }

        @Override
        public void run() {
            thread = Thread.currentThread();
            try {
                while (running && !isShutdown) {
                    idle = true;
                    Runnable task = queue.poll(keepAliveTime, TimeUnit.MILLISECONDS);
                    if (task == null) {
                        synchronized (workers) {
                            if (workers.size() > corePoolSize) {
                                LogHelper.info("[Worker] {} idle timeout, stopping.", thread.getName());
                                break;
                            }
                        }
                        continue;
                    }
                    idle = false;
                    LogHelper.info("[Worker] {} executes {}", thread.getName(), task);
                    try {
                        task.run();
                    } catch (Exception e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                boolean shouldAddWorker = false;
                int currentIdleCount = 0;
                synchronized (workers) {
                    workers.remove(this);
                    int activeCount = (int) workers.stream().filter(w -> !w.isIdle()).count();
                    currentIdleCount = workers.size() - activeCount;
                    if (currentIdleCount < minSpareThreads && workers.size() < maxPoolSize) {
                        shouldAddWorker = true;
                        addWorker();
                    }
                }
                if (shouldAddWorker) {
                    LogHelper.info("[Pool] Restored worker to maintain minSpareThreads (idle={}, required={})", currentIdleCount, minSpareThreads);
                }
                LogHelper.info("[Worker] {} terminated.", thread.getName());
                checkIfTerminated();
            }
        }
    }

    private static class MyThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(1);
        private final String prefix;

        MyThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            String name = prefix + "-worker-" + counter.getAndIncrement();
            Thread thread = new Thread(r, name);
            LogHelper.info("[ThreadFactory] Creating new thread: {}", name);
            return thread;
        }
    }
}