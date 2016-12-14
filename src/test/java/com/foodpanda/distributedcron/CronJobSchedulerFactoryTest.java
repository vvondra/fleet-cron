package com.foodpanda.distributedcron;

import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CronJobSchedulerFactoryTest {

    @Test
    public void testShutdownScheduler() {
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        final CronJobSchedulerFactory schedulerFactory = new CronJobSchedulerFactory(
            mock(Locker.class),
            executorService
        );

        final CronJobScheduler scheduler = schedulerFactory.createScheduler("test description");
        scheduler.stop();
        assertTrue(executorService.isShutdown());
    }

}
