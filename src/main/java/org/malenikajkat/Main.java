package org.malenikajkat;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
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

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("\n=== Тест 2: Перегрузка ===");
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

        System.out.println("\n=== Завершение пула ===");
        pool.shutdown();

        try {
            if (pool.awaitTermination(20, TimeUnit.SECONDS)) {
                System.out.println("✅ Все задачи завершены.");
            } else {
                System.out.println("❌ Таймаут ожидания завершения.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("❌ Ожидание прервано.");
        }

        try {
            pool.execute(() -> System.out.println("Эта задача не должна выполниться"));
        } catch (RejectedExecutionException e) {
            LogHelper.warn("✓ После shutdown задачи правильно отклоняются: " + e.getMessage());
        }

        System.out.println("=== Тест завершён ===");
    }
}