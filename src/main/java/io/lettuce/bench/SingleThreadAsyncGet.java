package io.lettuce.bench;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import io.lettuce.core.RedisCommandBuilder;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncGet extends AbstractSingleThreadAsync<byte[]> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncGet.class);
    }

    private final RedisCommandBuilder<byte[], byte[]> builder = new RedisCommandBuilder<>(ByteArrayCodec.INSTANCE);

    @Override
    protected RedisFuture<byte[]> doAsyncCommand(StatefulRedisConnection<byte[], byte[]> conn, byte[] key,
            byte[] ignoredValue) {
        return conn.async().get(key);
    }

    @Override
    protected Collection<RedisCommand<byte[], byte[], ?>> doAsyncCommands(StatefulRedisConnection<byte[], byte[]> conn,
            Collection<byte[]> keys, byte[] ignoredValue) {
        return conn.dispatch(keys.stream().map(key -> new AsyncCommand<>(builder.get(key))).collect(Collectors.toList()));
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, byte[] result) {
        LettuceAssert.assertState(Arrays.equals(value, result), String.format("value not match, exp: '%s', got: '%s'",
                new String(value), result == null ? "null" : new String(result)));
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
            new SingleThreadAsyncGet().test(useBatchFlush, loopNum, batchSize);
        }
        logger.info("=====================================");
    }

}
