package org.malenikajkat;

public class CallerRunsPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        LogHelper.warn("[Rejected] Task {} rejected, executing in caller thread", task);
        if (!executor.isShutdown()) {
            task.run();
        }
    }
}