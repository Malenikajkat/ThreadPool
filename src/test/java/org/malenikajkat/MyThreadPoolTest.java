package org.malenikajkat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class MyThreadPoolTest {
    private MyThreadPool pool;
    private ThreadFactoryImpl factory;

    @Before
    public void setUp() {
        factory = new ThreadFactoryImpl("TestPool");
        pool = new MyThreadPool(1, 2, 1, TimeUnit.SECONDS, 1, 1, factory);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (pool != null && !pool.isShutdown()) {
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testExecute_TaskExecuted() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        pool.execute(latch::countDown);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalStateException.class)
    public void testExecute_AfterShutdown_ThrowsException() {
        pool.shutdown();
        pool.execute(() -> {});
    }

    @Test
    public void testSubmit_ReturnsFuture() throws Exception {
        String expected = "Hello";
        Future<String> future = pool.submit(() -> expected);
        assertEquals(expected, future.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRejectedExecution_WithCallerRunsPolicy() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch callerLatch = new CountDownLatch(1);

        MyThreadPool limitedPool = new MyThreadPool(
                1, 1, 1, TimeUnit.SECONDS,
                1, 1, factory, new CallerRunsPolicy()
        );

        CountDownLatch queueLatch = new CountDownLatch(1);
        limitedPool.execute(() -> {
            try { Thread.sleep(100); } catch (InterruptedException e) {}
            queueLatch.countDown();
        });

        CountDownLatch overflowLatch = new CountDownLatch(1);
        limitedPool.execute(() -> {
            try { Thread.sleep(100); } catch (InterruptedException e) {}
            overflowLatch.countDown();
        });

        limitedPool.execute(() -> {
            counter.incrementAndGet();
            callerLatch.countDown();
        });

        assertTrue(queueLatch.await(5, TimeUnit.SECONDS));
        assertTrue(overflowLatch.await(5, TimeUnit.SECONDS));
        assertTrue(callerLatch.await(5, TimeUnit.SECONDS));
        assertEquals(1, counter.get());

        limitedPool.shutdown();
        limitedPool.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testShutdownNow_ClearsQueue() throws InterruptedException {
        CountDownLatch neverCalled = new CountDownLatch(1);

        MyThreadPool abortPool = new MyThreadPool(
                1, 1, 10, TimeUnit.SECONDS,
                10, 1, factory, new AbortPolicy()
        );

        abortPool.execute(() -> {
            try {
                Thread.sleep(5000);
                neverCalled.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        abortPool.shutdownNow();

        Thread.sleep(100);

        assertFalse("Задача была выполнена после shutdownNow!", neverCalled.await(1, TimeUnit.SECONDS));
        assertTrue(abortPool.awaitTermination(5, TimeUnit.SECONDS));
    }
}