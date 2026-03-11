package org.malenikajkat;

public class CallerRunsPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        if (!executor.isShutdown()) {
            task.run();
        }
    }
}