package org.malenikajkat;

import org.jetbrains.annotations.NotNull;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface CustomExecutor {
    void execute(@NotNull Runnable command);

    <T> Future<T> submit(@NotNull Callable<T> callable);

    void shutdown();

    @NotNull
    List<Runnable> shutdownNow();
}