package com.foodpanda.distributedcron;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CronJobSchedulerTest {

    /**
     * This is an integration test with a real instance
     */
    @Test
    public void itShouldScheduleAndRunACommandUsingTheWrappedScheduledExecutorService() throws InterruptedException {
        Runnable cronJob = mock(Runnable.class);

        // schedule to run immediately every millisecond
        buildRealTestObj().scheduleAtFixedRate(cronJob, 0, 1, TimeUnit.MILLISECONDS);

        // wait 100 millis
        Thread.sleep(100);

        // this is a bit sketchy, but verify the fake job ran at least a fourth of the number of times it could potentially run
        // i.e. test it runs multiple times, but keep the assertion lower bound conservative for test stability
        verify(cronJob, atLeast(25)).run();
    }

    /**
     * This is an integration test with a real instance
     */
    @Test
    public void itShouldKeepRunningWhenCronJobThrowsAnException() throws InterruptedException {
        Runnable cronJobThatThrowsException = mock(Runnable.class);
        doThrow(new RuntimeException("oh noes")).when(cronJobThatThrowsException).run();

        // schedule to run immediately every millisecond
        buildRealTestObj().scheduleAtFixedRate(cronJobThatThrowsException, 0, 1, TimeUnit.MILLISECONDS);

        // wait 100 millis
        Thread.sleep(100);

        // this is a bit sketchy, but verify the fake job ran at least a fourth of the number of times it could potentially run
        // i.e. test it runs multiple times, even with failures, but keep the assertion lower bound conservative for test stability
        verify(cronJobThatThrowsException, atLeast(25)).run();
    }

    /**
     * This is an integration test with a real instance
     */
    @Test
    public void itShouldScheduleAndRunACommandUsingTheWrappedScheduledExecutorServiceWithFixedDelay() throws InterruptedException {
        Runnable cronJob = mock(Runnable.class);

        // schedule to run immediately every millisecond
        buildRealTestObj().scheduleWithFixedDelay(cronJob, 0L, 1, TimeUnit.MILLISECONDS);

        Thread.sleep(150);

        // this is a bit sketchy, but verify the fake job ran at least a fourth of the number of times it could potentially run
        // i.e. test it runs multiple times, but keep the assertion lower bound conservative for test stability
        verify(cronJob, atLeast(25)).run();
    }

    /**
     * This is an integration test with a real instance
     */
    @Test
    public void itShouldKeepRunningWhenCronJobThrowsAnExceptionWithFixedDelay() throws InterruptedException {
        Runnable cronJobThatThrowsException = mock(Runnable.class);
        doThrow(new RuntimeException("oh noes")).when(cronJobThatThrowsException).run();

        // schedule to run immediately every millisecond
        buildRealTestObj().scheduleWithFixedDelay(cronJobThatThrowsException, 0L, 1, TimeUnit.MILLISECONDS);

        Thread.sleep(150);

        // this is a bit sketchy, but verify the fake job ran at least a fourth of the number of times it could potentially run
        // i.e. test it runs multiple times, even with failures, but keep the assertion lower bound conservative for test stability
        verify(cronJobThatThrowsException, atLeast(25)).run();
    }


    private CronJobScheduler buildRealTestObj() {
        final Locker locker = new Locker() {
            private Map<String, Boolean> map = new HashMap<>();

            @Override
            public boolean tryLock(String lockKey, int expiryInSeconds) {
                Boolean isLocked = map.get(lockKey);

                return isLocked == null || !isLocked;
            }

            @Override
            public void unlock(String lockKey) {
                map.put(lockKey, false);
            }

        };

        return new CronJobSchedulerFactory(locker)
            .createScheduler("test cron job");
    }
}
