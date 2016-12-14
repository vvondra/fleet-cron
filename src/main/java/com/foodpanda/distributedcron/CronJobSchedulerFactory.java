package com.foodpanda.distributedcron;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CronJobSchedulerFactory {

    private final Locker locker;

    private final ScheduledExecutorService executorService;

    public CronJobSchedulerFactory(final Locker locker) {
        this(
            locker,
            Executors.newSingleThreadScheduledExecutor()
        );
    }

    public CronJobSchedulerFactory(final Locker locker, final ScheduledExecutorService executorService) {
        this.locker = locker;
        this.executorService = executorService;
    }

    public CronJobScheduler createScheduler(final String schedulerDescription) {
        return new CronJobScheduler(
            locker,
            executorService,
            schedulerDescription
        );
    }
}
