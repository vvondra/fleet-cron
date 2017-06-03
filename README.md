# Fleet Cron

[![Build Status](https://travis-ci.org/foodpanda/fleet-cron.svg?branch=master)](https://travis-ci.org/foodpanda/fleet-cron)

This library provides a distributed lock based on DynamoDB. It uses [conditional writes](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate) to check if a lock can be acquired.

## Installation

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>com.foodpanda</groupId>
        <artifactId>fleet-cron</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

### Gradle

```groovy
compile 'com.foodpanda:fleet-cron:1.0.0'
```

## Example

### Just the locking mechanism

```java
Locker locker = new DynamoDbLocker(
    dynamoDbClient,
    "dynamoDbTable"
);

try {
    if (locker.tryLock("lockKey", 10)) {
        // do work
    }
} finally {
    locker.unlock("lockKey");
}
```

### Scheduling a cron running on multiple servers

```java

CronJobScheduler scheduler = new CronJobSchedulerFactory(locker).createScheduler("cron job name");

scheduler.scheduleAtFixedRate(
    () -> System.out.println("I did some work"),
    0, // Initial delay
    2, // Every 2 seconds,
    TimeUnit.SECONDS
);
```
