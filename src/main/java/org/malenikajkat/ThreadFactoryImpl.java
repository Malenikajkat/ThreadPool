package org.malenikajkat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryImpl implements ThreadFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger("ThreadFactory");
    private final String prefix;
    private final AtomicInteger count = new AtomicInteger(0);

    public ThreadFactoryImpl(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        int number = count.incrementAndGet();
        Thread thread = new Thread(r, prefix + "-worker-" + number);
        LogHelper.info("Создан новый поток: {}", thread.getName());
        return thread;
    }
}