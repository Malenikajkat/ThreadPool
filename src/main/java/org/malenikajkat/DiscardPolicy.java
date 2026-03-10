package org.malenikajkat;

public class DiscardPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        LogHelper.warn("Задача {} отброшена", task);
    }
}