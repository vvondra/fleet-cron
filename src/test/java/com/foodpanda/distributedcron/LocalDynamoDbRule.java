package com.foodpanda.distributedcron;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;

import org.junit.rules.ExternalResource;

class LocalDynamoDbRule extends ExternalResource {

    private AmazonDynamoDB amazonDynamoDB;

    @Override
    protected void before() throws Throwable {
        try {
            amazonDynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void after() {
        if (amazonDynamoDB == null) {
            return;
        }

        try {
            amazonDynamoDB.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    AmazonDynamoDB getDynamoDbClient() {
        return amazonDynamoDB;
    }
}
