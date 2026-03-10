package org.malenikajkat;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class TestMain {
    public static void main(String[] args) {
        MyThreadPool myThreadPool = new MyThreadPool(
                2, 4, 5L, TimeUnit.SECONDS,
                2, 2,
                new CallerRunsPolicy()
        );

        LogHelper.info("[Main] Submitting 10 tasks...");

        for (int i = 0; i < 10; i++) {
            final int taskNumber = i;
            try {
                myThreadPool.execute(() -> {
                    LogHelper.info("[Task] Executing task #{}", taskNumber);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            } catch (RejectedExecutionException e) {
                LogHelper.warn("[Rejected] Task {} was rejected!", taskNumber);
            }
        }

        LogHelper.info("[Main] Shutting down pool...");
        myThreadPool.shutdown();

        LogHelper.info("[Main] Waiting for termination (30 sec)...");
        try {
            if (myThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                LogHelper.info("[Main] ✅ All tasks completed.");
            } else {
                LogHelper.warn("[Main] ❌ Timeout: not all tasks finished.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LogHelper.warn("[Main] Interrupted while waiting.");
        }

        try {
            myThreadPool.execute(() -> LogHelper.info("[Task] This will never run."));
        } catch (RejectedExecutionException e) {
            LogHelper.warn("[Rejected] Attempt to submit after shutdown (expected): {}", e.getMessage());
        }

        LogHelper.info("[Main] 🎉 Test completed.");
    }
}