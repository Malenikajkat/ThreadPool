package org.malenikajkat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThreadPool implements CustomExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger("Pool");

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit unit;
    private final int minSpareThreads;
    private final int queueCapacity;

    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);
    private volatile boolean isShutdown = false;
    private final ThreadFactory threadFactory;
    private RejectedExecutionHandler rejectedHandler;

    public MyThreadPool(int corePoolSize,
                        int maxPoolSize,
                        long keepAliveTime,
                        TimeUnit unit,
                        int queueSize,
                        int minSpareThreads,
                        ThreadFactory threadFactory) {
        this(corePoolSize, maxPoolSize, keepAliveTime, unit, queueSize, minSpareThreads, threadFactory, new AbortPolicy());
    }

    public MyThreadPool(int corePoolSize,
                        int maxPoolSize,
                        long keepAliveTime,
                        TimeUnit unit,
                        int queueSize,
                        int minSpareThreads,
                        ThreadFactory threadFactory,
                        RejectedExecutionHandler handler) {
        if (corePoolSize <= 0 || maxPoolSize < corePoolSize || queueSize <= 0) {
            throw new IllegalArgumentException("Некорректная конфигурация: core > 0, max >= core, queue > 0");
        }

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.unit = unit;
        this.minSpareThreads = Math.min(minSpareThreads, corePoolSize);
        this.queueCapacity = queueSize;
        this.threadFactory = threadFactory;
        this.rejectedHandler = handler;

        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            throw new IllegalStateException("Пул потоков был остановлен.");
        }

        synchronized (this) {
            if (tryEnqueue(command)) {
                return;
            }

            if (workers.size() < maxPoolSize) {
                addWorker();
                if (tryEnqueue(command)) {
                    LogHelper.info("Задача принята после увеличения числа потоков");
                    return;
                }
            }

            rejectedHandler.rejected(command, this);
        }
    }

    private boolean tryEnqueue(Runnable command) {
        int size = queues.size();
        if (size == 0) return false;

        for (int i = 0; i < size; i++) {
            int index = nextQueueIndex.getAndIncrement() % size;
            BlockingQueue<Runnable> queue = queues.get(index);
            if (queue.offer(command)) {
                LogHelper.info("Задача добавлена в очередь #{}: {}", index, command);
                return true;
            }
        }
        return false;
    }

    private synchronized void addWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueCapacity);
        Worker worker = new Worker(queue, this);
        workers.add(worker);
        queues.add(queue);

        Thread thread = threadFactory.newThread(worker);
        thread.start();
    }

    synchronized void removeWorker(Worker worker) {
        workers.remove(worker);
        queues.remove(worker.getQueue());
        LogHelper.info("Воркер {} удалён из пула.", worker.getThreadName());
        notifyAll();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        interruptAllWorkers();
    }

    @Override
    public void shutdownNow() {
        isShutdown = true;
        for (BlockingQueue<Runnable> q : queues) {
            q.clear();
        }
        interruptAllWorkers();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanosTimeout = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanosTimeout;

        synchronized (this) {
            while (!workers.isEmpty()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false;
                }
                long millis = Math.max((remainingNanos) / 1_000_000, 1);
                wait(millis);
            }
        }
        return true;
    }

    private void interruptAllWorkers() {
        List<Worker> workersCopy;
        synchronized (this) {
            workersCopy = new ArrayList<>(workers);
        }
        for (Worker worker : workersCopy) {
            worker.stop();
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public boolean canRemoveWorker() {
        return workers.size() > corePoolSize;
    }

    public boolean shouldCreateSpareWorker() {
        return workers.size() < minSpareThreads;
    }

    static class Worker implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger("Worker");
        private final BlockingQueue<Runnable> taskQueue;
        private final MyThreadPool parent;
        private volatile boolean running = true;
        private Thread thread;

        public Worker(BlockingQueue<Runnable> taskQueue, MyThreadPool parent) {
            this.taskQueue = taskQueue;
            this.parent = parent;
        }

        @Override
        public void run() {
            this.thread = Thread.currentThread();

            try {
                while (running && !parent.isShutdown()) {
                    Runnable task = taskQueue.poll(parent.getKeepAliveTime(), parent.getUnit());
                    if (task != null) {
                        LogHelper.info("Выполняется задача: {}", task);
                        try {
                            task.run();
                        } catch (Exception e) {
                            LOGGER.error("Ошибка при выполнении задачи", e);
                        }
                    } else {
                        if (parent.canRemoveWorker()) {
                            LogHelper.info("Таймаут бездействия, остановка воркера.");
                            break;
                        }
                        if (parent.shouldCreateSpareWorker()) {
                            parent.addWorker();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.error("Неожиданная ошибка в воркере", e);
            } finally {
                parent.removeWorker(this);
            }
        }

        public void stop() {
            running = false;
            if (thread != null) {
                thread.interrupt();
            }
        }

        public BlockingQueue<Runnable> getQueue() {
            return taskQueue;
        }

        public String getThreadName() {
            return thread != null ? thread.getName() : "unknown";
        }
    }
}