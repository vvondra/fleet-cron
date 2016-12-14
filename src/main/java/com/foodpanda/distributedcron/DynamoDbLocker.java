package com.foodpanda.distributedcron;

import com.google.common.collect.ImmutableMap;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.OffsetDateTime;

public class DynamoDbLocker implements Locker {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbLocker.class);

    private static final String TABLE_KEY = "id";

    private static final String TABLE_CREATED_AT = "created_at";

    private static final String LOCK = "lock";

    private final DynamoDB dynamoDb;

    private final String tableName;

    private final Clock clock;

    public DynamoDbLocker(final DynamoDB dynamoDb, final String tableName) {
        this(dynamoDb, tableName, Clock.systemUTC());
    }

    public DynamoDbLocker(
        DynamoDB configuration,
        String tableName,
        Clock clock
    ) {
        this.dynamoDb = configuration;
        this.tableName = tableName;
        this.clock = clock;
    }

    /**
     * Acquires a distributed lock, returning true if it was acquired, false otherwise
     *
     * The lock, if acquired, has the passed expiry period in seconds. This means if another process attempts to
     * aquire it, it will fail until now + expiryInSeconds, after which time it will succeed. This prevents the lock
     * from being permanently locked, in case the acquiring process fails to release it for whatever reason.
     *
     * This is an atomic operation, only one process can acquire a lock at a time - if two processes contend
     * for a lock, only one will ever get a return value of true from this method.
     */
    @Override
    public boolean tryLock(String lockKey, int expiryInSeconds) {
        try {
            lockKey = getEnvironmentSpecificLockKey(lockKey);

            logger.info("Trying to acquire lock [{}]", lockKey);

            Table table = dynamoDb.getTable(tableName);

            Item lock = new Item()
                .withPrimaryKey(TABLE_KEY, lockKey)
                .withLong(LOCK, clock.millis() + (expiryInSeconds * 1000L))
                .withString(TABLE_CREATED_AT, OffsetDateTime.now(clock).toString());

            // create the lock if it doesn't exist, OR overwrite it if it's expired
            table.putItem(
                lock,
                "attribute_not_exists(#id) OR #lockExpiry < :now",
                ImmutableMap.of("#id", TABLE_KEY, "#lockExpiry", LOCK),
                ImmutableMap.of(":now", clock.millis())
            );

            logger.info("Acquired lock [{}]", lockKey);

            return true;
        } catch (ConditionalCheckFailedException e) { // thrown if we tried to acquire a locked lock
            logger.info("Could not acquire locked lock [{}]", lockKey);
        } catch (Exception ex) { // thrown on any other, unexpected, error performing the request
            logger.error("Error when trying to aquire lock [{}]: ", lockKey, ex);
        }

        return false;
    }

    /**
     * Release a distributed lock, by setting its expiry to 0
     *
     * This always succeeds. There should be no legitimate contention between processes for lock release, as long as
     * this is only called when you're *certain* that the process already holds the lock.
     */
    @Override
    public void unlock(String lockKey) {
        lockKey = getEnvironmentSpecificLockKey(lockKey);

        logger.info("Releasing lock [{}]", lockKey);

        Table table = dynamoDb.getTable(tableName);

        try {
            Item item = new Item()
                .withPrimaryKey(TABLE_KEY, lockKey)
                .withLong(LOCK, 0) // setting an expiry of 0 means the lock is always expired, therefore released
                .withString(TABLE_CREATED_AT, OffsetDateTime.now(clock).toString());

            table.putItem(item);
            logger.info("Released lock [{}]", lockKey);
        } catch (Exception ex) {
            logger.error("Failed to release lock [{}]", lockKey);
        }
    }

    /**
     * This allows different environments (dev, staging, prod) to store locks in the same DynamoDb instance
     *
     * @param lockKey the key for the lock
     * @return the key with the environment name appended, therefore unique to a (key,environment) pair
     */
    private String getEnvironmentSpecificLockKey(String lockKey) {
        String environment = System.getenv("ENV");
        String environmentKey = environment != null ? environment : "dev";

        return StringUtils.replace(lockKey + "_" + environmentKey, " ", "");
    }
}
