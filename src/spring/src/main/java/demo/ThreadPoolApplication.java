package demo;

import org.malenikajkat.CustomExecutor;
import org.malenikajkat.MyThreadPool;
import org.malenikajkat.ThreadFactoryImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ThreadPoolApplication {
    public static void main(String[] args) {
        SpringApplication.run(ThreadPoolApplication.class, args);
    }

    @Bean
    public CustomExecutor customExecutor() {
        return new MyThreadPool(
                2, 4, 10, TimeUnit.SECONDS,
                5, 2, new ThreadFactoryImpl("HttpPool"),
                new org.malenikajkat.CallerRunsPolicy()
        );
    }
}

@RestController
class TaskController {
    private final CustomExecutor executor;

    TaskController(CustomExecutor executor) {
        this.executor = executor;
    }

    @GetMapping("/task")
    public String submitTask() {
        executor.execute(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("Фоновая задача завершена");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return "Задача отправлена";
    }
}