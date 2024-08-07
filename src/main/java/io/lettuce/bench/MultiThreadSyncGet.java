package io.lettuce.bench;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.bench.utils.BenchUtils;
import io.lettuce.core.AutoBatchFlushOptions;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncGet {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MultiThreadSyncGet.class);

    private static final int THREAD_COUNT = 32;

    private static final int LOOP_NUM = 500_000;

    private static final int DIGIT_NUM = 9;

    private static final String KEY_FORMATTER = String.format("key-%%0%dd", DIGIT_NUM);

    private static final String VALUE_FORMATTER = String.format("value-%%0%dd", DIGIT_NUM);

    static {
        // noinspection ConstantValue
        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(LOOP_NUM).length() + 1, "digit num is not large enough");
    }

    void test(boolean useBatchFlush) {
        try (RedisClient redisClient = RedisClient
                .create(RedisURI.create("test-cluster-0001-001.p24bb1.0001.apse2.cache.amazonaws.com", 6379))) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            if (useBatchFlush) {
                optsBuilder.autoBatchFlushOptions(AutoBatchFlushOptions.builder().enableAutoBatchFlush(true).build());
            }
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            logger.info("thread count: {}", THREAD_COUNT);
            final Thread[] threads = new Thread[THREAD_COUNT];
            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();
            for (int i = 0; i < THREAD_COUNT; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < LOOP_NUM; j++) {
                        final long cmdStart = System.nanoTime();
                        final byte[] resultBytes = connection.sync().get(genKey(j));
                        totalLatency.addAndGet((System.nanoTime() - cmdStart) / 1000);
                        LettuceAssert.assertState(Arrays.equals(genValue(j), resultBytes), "value not match");
                        totalCount.incrementAndGet();
                    }
                });
            }
            final long start = System.nanoTime();
            Arrays.asList(threads).forEach(Thread::start);
            Arrays.asList(threads).forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            });
            double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
            logger.info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
            logger.info("Total time: {}s", costInSeconds);
            logger.info("Avg latency: {}us", totalLatency.get() / (double) totalCount.get());
            logger.info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
            BenchUtils.logEnterRatioIfNeeded(logger);
            BenchUtils.logAvgBatchCount(logger);
        }
    }

    private byte[] genKey(int j) {
        return String.format(KEY_FORMATTER, j).getBytes();
    }

    private byte[] genValue(int j) {
        return String.format(VALUE_FORMATTER, j).getBytes();
    }

    public static void main(String[] args) {
        for (boolean useBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("useBatchFlush: {}", useBatchFlush);
            new MultiThreadSyncGet().test(useBatchFlush);
        }
        logger.info("=====================================");
    }

}
