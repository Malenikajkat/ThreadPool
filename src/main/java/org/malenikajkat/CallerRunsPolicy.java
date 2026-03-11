package org.malenikajkat;

public class CallerRunsPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        if (!executor.isShutdown()) {
            LogHelper.warn("[Rejected] Running task {} in caller thread due to overload", task);
            task.run();
        }
    }
}