package org.malenikajkat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class TestMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestMain.class);

    public static void main(String[] args) {
        ThreadFactoryImpl threadFactory = new ThreadFactoryImpl("MyPool");
        MyThreadPool myThreadPool = new MyThreadPool(
                2,
                4,
                5L,
                TimeUnit.SECONDS,
                2,
                2,
                threadFactory
        );

        LogHelper.info("Отправка 10 задач...");

        for (int i = 0; i < 10; i++) {
            final int taskNumber = i;
            try {
                myThreadPool.execute(() -> {
                    LogHelper.info("Выполняется задача номер {}", taskNumber);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            } catch (RejectedExecutionException e) {
                LogHelper.warn("Задача {} отклонена из-за перегрузки!", taskNumber);
            }
        }

        LogHelper.info("Остановка пула потоков...");
        myThreadPool.shutdown();

        LogHelper.info("Ожидание завершения всех задач (до 30 секунд)...");
        try {
            if (myThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                LogHelper.info("✅ Все задачи успешно завершены.");
            } else {
                LogHelper.warn("❌ Таймаут: не все задачи завершились за 30 секунд.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LogHelper.warn("Прерывание во время ожидания завершения.");
        }

        try {
            myThreadPool.execute(() -> LogHelper.info("Эта задача никогда не выполнится."));
        } catch (IllegalStateException ise) {
            LogHelper.warn("Попытка отправить задачу после остановки (ожидаемо): {}", ise.getMessage());
        }

        LogHelper.info("🎉 Тест завершён.");
    }
}