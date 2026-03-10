package dev.ak.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TokenBucketRateLimiterTest {

    private static RedisContainer redisContainer;
    private Jedis jedis;
    private TokenBucketRateLimiter rateLimiter;

    @BeforeAll
    static void startContainer() {
        redisContainer = new RedisContainer("redis:latest");
        redisContainer.withExposedPorts(6379).start();
    }

    @AfterAll
    static void stopContainer() {
        redisContainer.stop();
    }

    @BeforeEach
    void setup() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    void tearDown() {
        jedis.close();
    }

    @Test
    public void shouldAllowRequestsWithinBucketCapacity() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0); // 5 tokens, 1 token/sec
        for (int i = 0; i < 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("client-1 should be allowed within the capacity limit")
                    .isTrue();
        }
    }

    @Test
    public void shouldDenyRequestsOnceBucketIsEmpty() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0); // 5 tokens, 1 token/sec
        for (int i = 0; i < 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("client-1 should be allowed within the capacity limit")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();
    }

    @Test
    public void shouldAllowRequestsAfterTokensAreRefilled() throws InterruptedException {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0); // 5 tokens, 1 token/sec
        for (int i = 0; i < 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("client-1 should be allowed within the capacity limit")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        TimeUnit.SECONDS.sleep(1); // wait for tokens to refill

        assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request after tokens are refilled should be allowed")
                .isTrue();
    }

    @Test
    void shouldHandleMultipleClientsIndependently() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);

        String clientId1 = "client-1";
        String clientId2 = "client-2";

        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request %d should be allowed", i)
                    .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond bucket capacity should be denied")
                .isFalse();

        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request %d should be allowed", i)
                    .isTrue();
        }
    }

}