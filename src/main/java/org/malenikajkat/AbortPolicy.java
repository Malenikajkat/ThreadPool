package org.malenikajkat;

import java.util.concurrent.RejectedExecutionException;

public class AbortPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        LogHelper.warn("[Rejected] Task {} was rejected due to overload!", task);
        throw new RejectedExecutionException("Task " + task + " was rejected by the pool");
    }
}