package com.foodpanda.distributedcron;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamoDbLockerTest {

    @ClassRule
    public static final LocalDynamoDbRule dynamoDB = new LocalDynamoDbRule();

    private static final String LOCK_KEY = "lock";

    private String tableName;

    private int tableCount = 0;

    private Locker locker;

    @Before
    public void setUp() {
        // Unique table for each run
        tableName = "table" + String.valueOf(tableCount++);
        dynamoDB.getDynamoDbClient().createTable(
            new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", "HASH"))
                .withAttributeDefinitions(
                    new AttributeDefinition("id", "S")
                )
                .withProvisionedThroughput(
                    new ProvisionedThroughput(1L, 1L)
                )
        );

        locker = new DynamoDbLocker(
            new DynamoDB(dynamoDB.getDynamoDbClient()),
            tableName,
            Clock.systemUTC()
        );
    }

    @After
    public void tearDown() {
        dynamoDB.getDynamoDbClient().deleteTable(tableName);
    }

    @Test
    public void testTryLockDoesNotReacquire() {
        assertTrue(locker.tryLock(LOCK_KEY, 20));
        assertFalse(locker.tryLock(LOCK_KEY, 20));
    }

    @Test
    public void testUnrelatedLocks() {
        assertTrue(locker.tryLock(LOCK_KEY, 20));
        assertTrue(locker.tryLock(LOCK_KEY + LOCK_KEY, 20));
    }


    @Test
    public void testExpiresLock() throws InterruptedException {
        assertTrue(locker.tryLock(LOCK_KEY, 1));
        Thread.sleep(1500L);
        assertTrue(locker.tryLock(LOCK_KEY, 1));
    }


    @Test
    public void testUnlock() {
        assertTrue(locker.tryLock(LOCK_KEY, 20));
        locker.unlock(LOCK_KEY);
        assertTrue(locker.tryLock(LOCK_KEY, 20));
    }

}
