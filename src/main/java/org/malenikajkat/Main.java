package org.malenikajkat;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        MyThreadPool pool = new MyThreadPool(
                2, 4, 5, TimeUnit.SECONDS, 5, 1,
                new CallerRunsPolicy()
        );

        System.out.println("=== Тест 1: Нормальная работа ===");
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    System.out.println("[Задача " + taskId + "] Запущена");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.println("[Задача " + taskId + "] Завершена");
                });
            } catch (RejectedExecutionException e) {
                LogHelper.warn("[Задача " + taskId + "] Отклонена: " + e.getMessage());
            }
        }

        Thread.sleep(3000);

        System.out.println("\n=== Тест 2: Перегрузка (выполняются в вызывающем потоке) ===");
        for (int i = 10; i < 30; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    System.out.println("[Перегрузка " + taskId + "] Запущена");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.println("[Перегрузка " + taskId + "] Завершена");
                });
            } catch (RejectedExecutionException e) {
                LogHelper.warn("[Перегрузка " + taskId + "] Отклонена: " + e.getMessage());
            }
        }

        Thread.sleep(5000);

        System.out.println("\n=== Тест 3: Submit с Future ===");
        try {
            java.util.concurrent.Future<String> future = pool.submit(() -> {
                Thread.sleep(1000);
                return "Результат из Future";
            });

            System.out.println("Результат Future: " + future.get());
        } catch (Exception e) {
            LogHelper.warn("Ошибка при выполнении задачи через Future", e);
        }

        Thread.sleep(2000);

        System.out.println("\n=== Завершение пула ===");
        pool.shutdown();

        try {
            pool.execute(() -> System.out.println("Эта задача не должна выполниться"));
        } catch (RejectedExecutionException e) {
            LogHelper.warn("✓ После shutdown задачи правильно отклоняются: " + e.getMessage());
        }

        Thread.sleep(1000);
        System.out.println("=== Тест завершён ===");
    }
}