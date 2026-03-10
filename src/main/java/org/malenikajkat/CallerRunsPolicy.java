package org.malenikajkat;

public class CallerRunsPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        LogHelper.warn("Задача {} отклонена, выполняется в вызывающем потоке", task);
        if (!executor.isShutdown()) {
            task.run();
        }
    }
}