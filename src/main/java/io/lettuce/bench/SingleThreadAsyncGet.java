package io.lettuce.bench;

import java.util.Arrays;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncGet extends AbstractSingleThreadAsync<byte[]> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncGet.class);
    }

    @Override
    protected RedisFuture<byte[]> doAsyncCommand(RedisAsyncCommands<byte[], byte[]> async, byte[] key, byte[] ignoredValue) {
        return async.get(key);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, byte[] result) {
        // LettuceAssert.assertState(Arrays.equals(value, result), String.format("value not match, exp: '%s', got: '%s'",
        // new String(value), result == null ? "null" : new String(result)));
    }

    public static void main(String[] args) {
        for (boolean useBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("useBatchFlush: {}", useBatchFlush);
            new SingleThreadAsyncGet().test(useBatchFlush);
        }
        logger.info("=====================================");
    }

}
