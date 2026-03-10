package dev.ak.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;


class SlidingWindowLogRateLimiterTest {

    private static final RedisContainer redisContainer = new RedisContainer("redis:latest")
            .withExposedPorts(6379);

    private Jedis jedis;
    private SlidingWindowLogRateLimiter rateLimiter;

    static {
        redisContainer.start();
    }

    @BeforeEach
    public void setup() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    public void tearDown() {
        jedis.close();
    }

    @Test
    public void shouldAllowRequestsWithinLimit() {
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, 5, 10);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }
    }

    @Test
    public void shouldDenyRequestsOnceLimitIsExceeded() {
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, 5, 60);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();
    }

    @Test
    public void shouldAllowRequestsAgainAfterSlidingWindowResets() throws InterruptedException {
        int limit = 5;
        String clientId = "client-1";
        long windowSize = 1L;
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, limit, windowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep((windowSize + 1) * 1000);

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request after window reset should be allowed")
                .isTrue();
    }
}