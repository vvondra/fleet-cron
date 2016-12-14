package com.foodpanda.distributedcron;

public interface Locker {

    /**
     * Acquires a  lock with some expiry period in seconds, returning true if it was required,
     * false otherwise. The acquiring tries one, does not wait to retry acquiring the lock.
     *
     * @param lockKey the key for the lock
     * @param expiryInSeconds the expiry period of the lock, in seconds
     * @return true if the lock was acquired, false otherwise
     */
    boolean tryLock(String lockKey, int expiryInSeconds);

    /**
     * Release a distributed lock
     *
     * @param lockKey the key for the lock
     */
    void unlock(String lockKey);
}
