
package io.lettuce.bench;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncSet extends AbstractSingleThreadAsync<String> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncSet.class);
    }

    @Override
    protected RedisFuture<String> doAsyncCommand(RedisAsyncCommands<byte[], byte[]> async, byte[] key, byte[] value) {
        return async.set(key, value);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, String result) {
        // do nothing
    }

    public static void main(String[] args) {
        for (boolean useBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("useBatchFlush: {}", useBatchFlush);
            new SingleThreadAsyncSet().test(useBatchFlush);
        }
        logger.info("=====================================");
    }

}
