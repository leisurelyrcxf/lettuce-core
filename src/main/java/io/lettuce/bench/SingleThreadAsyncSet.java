package io.lettuce.bench;

import java.util.Collection;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncSet extends AbstractSingleThreadAsync<String> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncSet.class);
    }

    @Override
    protected RedisFuture<String> doAsyncCommand(StatefulRedisConnection<byte[], byte[]> conn, byte[] key, byte[] value) {
        return conn.async().set(key, value);
    }

    @Override
    protected Collection<RedisCommand<byte[], byte[], ?>> doAsyncCommands(StatefulRedisConnection<byte[], byte[]> conn,
            Collection<byte[]> keys, byte[] ignoredValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, String result) {
        // do nothing
    }

    public static void main(String[] args) {
        int loopNum = 0;
        int batchSize = 0;
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
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
        if (loopNum == 0) {
            throw new IllegalArgumentException("loop num must be specified");
        }
        if (batchSize == 0) {
            throw new IllegalArgumentException("batch size must be specified");
        }
        logger.info("loop num: {}", loopNum);
        logger.info("batch size: {}", batchSize);
        for (boolean useBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("useBatchFlush: {}", useBatchFlush);
            new SingleThreadAsyncSet().test(useBatchFlush, loopNum, batchSize);
        }
        logger.info("=====================================");
    }

}
