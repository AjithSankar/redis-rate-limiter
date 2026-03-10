package dev.ak.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.*;

class FixedWindowRateLimiterTest {

    private static final RedisContainer redisContainer = new RedisContainer("redis:latest")
            .withExposedPorts(6379);

    private Jedis jedis;
    private FixedWindowRateLimiter rateLimiter;

    static {
        redisContainer.start();
    }

    @BeforeEach
    void setUp() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    void tearDown() {
        jedis.close();
    }

    @AfterAll
    static void stopContainer() {
        redisContainer.stop();
    }

    @Test
    public void shouldAllowRequestsWithinLimit() {
        rateLimiter = new FixedWindowRateLimiter(jedis, 10, 5);
        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.isAllowed("client-1"), "Request " + i + " should have been allowed.");
        }
    }

    @Test
    public void shouldDenyRequestsOnceLimitIsExceeded() {
        rateLimiter = new FixedWindowRateLimiter(jedis, 10, 5);
        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.isAllowed("client-1"), "Request " + i + " should have been allowed.");
        }

        assertFalse(rateLimiter.isAllowed("client-1"), "Requests should not be allowed.");
    }

    @Test
    public void shouldAllowRequestsAfterFixedWindowResets() throws InterruptedException {

        int limit = 5;
        int windowSize = 1;
        String clientId = "client-1";

        rateLimiter = new FixedWindowRateLimiter(jedis, windowSize, limit);
        for (int i = 1; i <= limit; i++) {
            assertTrue(rateLimiter.isAllowed(clientId), "Request " + i + " should have been allowed.");  // window start
        }

        assertFalse(rateLimiter.isAllowed(clientId), "Requests beyond limits should not be allowed."); // window end

        Thread.sleep((windowSize + 1) * 1000);

        assertTrue(rateLimiter.isAllowed(clientId), "Request after window reset should be allowed."); // after window reset

    }

    @Test
    public void shouldHandleMultipleClientsIndependently() throws InterruptedException {
        int limit = 5;
        int windowSize = 10;
        String clientId1 = "client-1";
        String clientId2 = "client-2";

        rateLimiter = new FixedWindowRateLimiter(jedis, windowSize, limit);

        for (int i = 1; i <= limit; i++) {
            assertTrue(rateLimiter.isAllowed(clientId1), "client 1 should have been allowed.");  // window start
        }

        assertFalse(rateLimiter.isAllowed(clientId1),"Client 1 request beyond limit should be denied");

        for (int i = 1; i <= limit; i++) {
            assertTrue(rateLimiter.isAllowed(clientId2), "client2 should have been allowed.");  // window start

        }
    }
}