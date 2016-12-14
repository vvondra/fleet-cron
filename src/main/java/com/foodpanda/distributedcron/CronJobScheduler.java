package com.foodpanda.distributedcron;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for our managed cron jobs. it wraps the ScheduledExecutorService which runs a cron
 * job command at a fixed rate. This wrapper implementation ensures that any exception thrown by the
 * cron job command is caught, logged, and swallowed.
 *
 * If uncaught, any exception will stop the ScheduledExecutorService entirely. This means that any
 * unhandled exception thrown by any cron job will halt future runs of that particular job, but also
 * all other jobs. Ideally, we would like to handle all exceptions thrown by all our cron jobs -
 * this just provides a final safety net in case any slip through.
 */
public class CronJobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(CronJobScheduler.class);

    private static final int DEFAULT_CRONJOB_EXPIRY = 600;

    private final Locker locker;

    private final ScheduledExecutorService scheduledExecutorService;

    private final String cronJobDescription;

    CronJobScheduler(
        Locker locker,
        ScheduledExecutorService scheduledExecutorService,
        String cronJobDescription
    ) {
        this.locker = locker;
        this.scheduledExecutorService = scheduledExecutorService;
        this.cronJobDescription = cronJobDescription;
    }

    public void scheduleAtFixedRateWithLock(Runnable command, long initialDelay, long period, TimeUnit unit, int expiryInSeconds) {
        scheduledExecutorService.scheduleAtFixedRate(runAndReleaseLockAfterTimeout(command, expiryInSeconds), initialDelay, period, unit);

        logger.info("Scheduled cron job '{}' with a start delay of {}, a fixed rate of {} and expiry in {} seconds",
                    cronJobDescription,
                    initialDelay +  " " + unit.name(),
                    period +  " " + unit.name(),
                    expiryInSeconds
        );
    }

    public void scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(runAndReleaseLockAfterCommand(command), initialDelay, period, unit);

        logger.info("Scheduled cron job '{}' with a start delay of {} and a fixed rate of {}",
            cronJobDescription,
            initialDelay +  " " + unit.name(),
            period +  " " + unit.name()
        );
    }

    public void scheduleWithFixedDelay(final Runnable command, final Long initialDelay, final Integer breakBetweenRunsInSeconds, final TimeUnit unit) {
        scheduledExecutorService.scheduleWithFixedDelay(runAndReleaseLockAfterCommand(command), initialDelay, breakBetweenRunsInSeconds, unit);

        logger.info("Scheduled cron job '{}' with a start delay of {} and a delay before next run of {}",
            cronJobDescription,
            initialDelay +  " " + unit.name(),
            breakBetweenRunsInSeconds +  " " + unit.name()
        );
    }

    public void stop() {
        scheduledExecutorService.shutdown();

        logger.info("Shut down schedule of cron job '{}'", cronJobDescription);
    }

    /**
     * Run the command, catching, logging and then swallowing any exception that gets thrown
     *
     * This guarantees only one instance of the job will be run at a given time
     *
     * The lock is released after the command is done
     */
    private Runnable runAndReleaseLockAfterCommand(Runnable command) {
        return () -> {
            logger.info("Started run of cron job '{}'", cronJobDescription);
            final String lockKey = "LOCK_" + cronJobDescription;

            try {
                // DEFAULT_CRONJOB_EXPIRY is just a safety net if the lock still hasn't been released.
                if (locker.tryLock(lockKey, DEFAULT_CRONJOB_EXPIRY)) {
                    command.run();

                    logger.info("Finished run of cron job '{}'", cronJobDescription);
                } else {
                    logger.info("Cron job {} already running cannot acquire lock", cronJobDescription);
                }
            } catch (Exception ex) {
                logger.error("Cron job '{}' run failed with an unhandled exception: {}",
                    cronJobDescription,
                    ex.getMessage(),
                    ex
                );
            } finally {
                locker.unlock(lockKey);
            }
        };
    }

    /**
     * Run the command, catching, logging and then swallowing any exception that gets thrown
     *
     * This guarantees only one instance of the job will be run at a given time
     *
     * The lock is released after the expiryInSeconds finishes
     */
    private Runnable runAndReleaseLockAfterTimeout(Runnable command, int expiryInSeconds) {
        return () -> {
            logger.info("Started run of cron job '{}'", cronJobDescription);
            final String lockKey = "LOCK_" + cronJobDescription;

            try {
                if (locker.tryLock(lockKey, expiryInSeconds)) {
                    command.run();

                    logger.info("Finished run of cron job '{}'", cronJobDescription);
                } else {
                    logger.info("Cron job {} already running cannot acquire lock", cronJobDescription);
                }
            } catch (Exception ex) {
                logger.error("Cron job '{}' run failed with an unhandled exception: {}",
                     cronJobDescription,
                     ex.getMessage(),
                     ex
                );
            }
        };
    }
}
