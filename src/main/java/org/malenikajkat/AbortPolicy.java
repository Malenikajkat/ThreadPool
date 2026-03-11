package org.malenikajkat;

import java.util.concurrent.RejectedExecutionException;

public class AbortPolicy implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, MyThreadPool executor) {
    }
}