package org.malenikajkat;

@FunctionalInterface
public interface RejectedExecutionHandler {
    void rejected(Runnable task, MyThreadPool executor);
}