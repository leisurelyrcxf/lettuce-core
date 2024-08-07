package io.lettuce.bench;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.bench.utils.BenchUtils;
import io.lettuce.core.AutoBatchFlushOptions;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLogger;

@SuppressWarnings({ "ConstantValue", "BusyWait" })
abstract class AbstractSingleThreadAsync<T> {

    protected static InternalLogger logger;

    private static InternalLogger getLogger() {
        return logger;
    }

    private static final int LOOP_NUM = 10_000_000;

    private static final int DIGIT_NUM = 9;

    private static final String KEY_FORMATTER = String.format("key-%%0%dd", DIGIT_NUM);

    private static final String VALUE_FORMATTER = String.format("value-%%0%dd", DIGIT_NUM);

    static {
        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(LOOP_NUM).length() + 1, "digit num is not large enough");
    }

    String prevKey = "";

    private byte[] genKey(int j) {
        return String.format(KEY_FORMATTER, j).getBytes();
    }

    private byte[] genValue(int j) {
        return String.format(VALUE_FORMATTER, j).getBytes();
    }

    protected abstract RedisFuture<T> doAsyncCommand(RedisAsyncCommands<byte[], byte[]> async, byte[] key, byte[] value);

    protected abstract void assertResult(byte[] key, byte[] value, T result);

    protected final void test(boolean useBatchFlush) {
        try (RedisClient redisClient = RedisClient
                .create(RedisURI.create("test-cluster-0001-001.p24bb1.0001.apse2.cache.amazonaws.com", 6379))) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            if (useBatchFlush) {
                optsBuilder.autoBatchFlushOptions(AutoBatchFlushOptions.builder().enableAutoBatchFlush(true).busyLoop(false)
                        .busyLoopDelayInNanos(10).batchSize(8).build());
            }
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();

            final long start = System.nanoTime();
            for (int j = 0; j < LOOP_NUM; j++) {
                final long cmdStart = System.nanoTime();
                final byte[] keyBytes = genKey(j);
                final byte[] valueBytes = genValue(j);
                final String key = new String(keyBytes);
                final RedisFuture<T> resultFut = doAsyncCommand(connection.async(), keyBytes, valueBytes);
                resultFut.whenComplete((result, throwable) -> {
                    try {
                        if (throwable != null) {
                            getLogger().error("async#get failed: key: {}, err: {}", throwable.getMessage(), keyBytes,
                                    throwable);
                        }
                        assertResult(keyBytes, valueBytes, result);
                        totalCount.incrementAndGet();
                        totalLatency.addAndGet((System.nanoTime() - cmdStart) / 1000);

                        LettuceAssert.assertState(key.compareTo(prevKey) > 0,
                                String.format("not in order, prevKey: %s, key: %s", prevKey, key));
                        prevKey = key;
                    } catch (Exception e) {
                        getLogger().error("async#get failed: key: {}, err: {}", key, e.getMessage(), e);
                    }
                });
            }
            while (totalCount.get() != LOOP_NUM) {
                Thread.sleep(1);
            }
            double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
            getLogger().info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
            getLogger().info("Total time: {}s", costInSeconds);
            getLogger().info("Avg latency: {}s", totalLatency.get() / (double) totalCount.get() / 1000.0 / 1000.0);
            getLogger().info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
            BenchUtils.logEnterRatioIfNeeded(logger);
            BenchUtils.logAvgBatchCount(logger);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

}
