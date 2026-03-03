# Redis Rate Limiter Java Example

This repository showcases various implementations of rate limiters using Redis and Java with the Jedis library. Rate limiting is a crucial technique for managing traffic, preventing abuse, and ensuring fair usage of resources in distributed systems.

## Features

The project implements several rate-limiting algorithms, including:
•	Fixed Window Rate Limiter
•	Leaky Bucket Rate Limiter
•	Sliding Window Counter Rate Limiter
•	Sliding Window Log Rate Limiter
•	Token Bucket Rate Limiter

Rate Limiter Implementations

1. Fixed Window
- Limits requests in fixed intervals (e.g., 100 requests per minute).
- Strength: Simplicity.
- Weakness: Burst traffic at window edges.

2. Leaky Bucket
- Allows a constant flow of requests, regardless of bursts.
- Strength: Smooth request handling.
- Weakness: Potential delays for valid requests.

3. Sliding Window Counter
- Tracks request counts in sliding time intervals.
- Strength: Reduces burst issues.
- Weakness: Higher complexity and Redis writes.

4. Sliding Window Log
- Logs each request timestamp and checks against the rate limit.
- Strength: Accurate rate-limiting.
- Weakness: High storage requirement for logs.

5. Token Bucket
- Uses tokens to allow requests up to a burst limit, refilling at a steady rate.
- Strength: Flexible for burst and sustained traffic.
- Weakness: Slightly more complex implementation.
