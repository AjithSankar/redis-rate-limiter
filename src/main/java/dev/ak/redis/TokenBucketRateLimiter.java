package dev.ak.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class TokenBucketRateLimiter {

    private Jedis jedis;
    private double refillRate; // tokens refilled per second
    private int bucketCapacity; // max tokens in the bucket

    public TokenBucketRateLimiter(Jedis jedis, int bucketCapacity, double refillRate) {
        this.jedis = jedis;
        this.refillRate = refillRate;
        this.bucketCapacity = bucketCapacity;
    }

    public boolean isAllowed(String clientId) {
        String keyCount = "rate_limit:" + clientId + ":count";
        String keyLastRefill = "rate_limit:" + clientId + ":lastRefill";

        long currentTime = System.currentTimeMillis();

        // fetch current token count and last refill time

        Transaction transaction = jedis.multi();
        transaction.get(keyLastRefill);
        transaction.get(keyCount);
        var results = transaction.exec();

        long lastRefillTime = results.get(0) != null ? Long.parseLong((String)results.get(0)) : currentTime;
        int tokenCount = results.get(1) != null ? Integer.parseInt((String)results.get(1)) : bucketCapacity;

        // Refill tokens based on elapsed time
        long elapsedTimeMs = currentTime - lastRefillTime;
        double elapsedTimeSeconds = elapsedTimeMs / 1000.0;
        int tokensToAdd = (int)(elapsedTimeSeconds * refillRate);
        tokenCount = Math.min(tokenCount + tokensToAdd, bucketCapacity);

        boolean isAllowed = tokenCount > 0;

        if (isAllowed) {
            tokenCount--; // consume a token
        }

        // update redis state
        transaction = jedis.multi();
        transaction.set(keyLastRefill, String.valueOf(currentTime));
        transaction.set(keyCount, String.valueOf(tokenCount));
        transaction.exec();

        return isAllowed;
    }

}
