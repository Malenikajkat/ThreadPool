package org.malenikajkat;

@SuppressWarnings("unused")
public class DiscardPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
        LogHelper.warn("[Rejected] Task {} discarded", task);
    }
}