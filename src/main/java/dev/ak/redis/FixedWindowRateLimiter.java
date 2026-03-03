package dev.ak.redis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ExpiryOption;

public class FixedWindowRateLimiter {

    private final Logger log = LogManager.getLogger(FixedWindowRateLimiter.class);

    private final Jedis jedis;
    private final int windowSize;  // Time window
    private final int limit;  // Max requests

    public FixedWindowRateLimiter(Jedis jedis, int windowSize, int limit) {
        this.jedis = jedis;
        this.windowSize = windowSize;
        this.limit = limit;
    }

    public boolean isAllowed(String clientId) {
        String key = "rate_limit:" + clientId;
        String currentCountStr = jedis.get(key);

        int currentCount = currentCountStr != null ? Integer.parseInt(currentCountStr) : 0;
        log.info("Current count is " + currentCount);
        boolean isAllowed = currentCount < limit;

        if (isAllowed) {
            log.info("Client " + clientId + " is allowed.");
            Transaction transaction = jedis.multi();
            transaction.incr(key);
            transaction.expire(key, windowSize, ExpiryOption.NX);
            transaction.exec();
        } else {
            log.info("Client " + clientId + " is disallowed.");
        }

        return isAllowed;
    }
}
