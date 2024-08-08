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
import io.lettuce.core.protocol.DefaultBatchFlushEndpoint;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncExists {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MultiThreadSyncExists.class);

    private static final int DIGIT_NUM = 9;

    private static final String KEY_FORMATTER = String.format("key-%%0%dd", DIGIT_NUM);

    private static final String VALUE_FORMATTER = String.format("value-%%0%dd", DIGIT_NUM);

    void test(boolean useBatchFlush, int threadCount, int loopNum, int batchSize) {
        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(loopNum).length() + 1, "digit num is not large enough");

        DefaultBatchFlushEndpoint.FLUSHED_COMMAND_COUNT.set(0L);
        DefaultBatchFlushEndpoint.FLUSHED_BATCH_COUNT.set(0L);
        try (RedisClient redisClient = RedisClient
                .create(RedisURI.create("test-cluster-0001-002.p24bb1.0001.apse2.cache.amazonaws.com", 6379))) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            if (useBatchFlush) {
                optsBuilder.autoBatchFlushOptions(
                        AutoBatchFlushOptions.builder().enableAutoBatchFlush(true).batchSize(batchSize).build());
            }
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final Thread[] threads = new Thread[threadCount];
            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < loopNum; j++) {
                        final long cmdStart = System.nanoTime();
                        connection.sync().exists(genKey(j));
                        totalLatency.addAndGet((System.nanoTime() - cmdStart) / 1000);
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
        int threadCount = 0;
        int loopNum = 0;
        int batchSize = 0;
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "-t":
                    i++;
                    threadCount = Integer.parseInt(args[i]);
                    break;
                case "-n":
                    i++;
                    loopNum = Integer.parseInt(args[i]);
                    break;
                case "-b":
                    i++;
                    batchSize = Integer.parseInt(args[i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + args[i]);
            }
            i++;
        }
        if (threadCount == 0) {
            throw new IllegalArgumentException("thread count must be specified");
        }
        if (loopNum == 0) {
            throw new IllegalArgumentException("loop num must be specified");
        }
        if (batchSize == 0) {
            throw new IllegalArgumentException("batch size must be specified");
        }
        logger.info("thread count: {}", threadCount);
        logger.info("loop num: {}", loopNum);
        logger.info("batch size: {}", batchSize);
        for (boolean useBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("useBatchFlush: {}", useBatchFlush);
            new MultiThreadSyncExists().test(useBatchFlush, threadCount, loopNum, batchSize);
        }
        logger.info("=====================================");
    }

}
