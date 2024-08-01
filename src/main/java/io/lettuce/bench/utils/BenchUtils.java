package io.lettuce.bench.utils;

import io.lettuce.core.context.BatchFlushEndPointContext;
import io.lettuce.core.protocol.DefaultBatchFlushEndpoint;
import io.netty.util.internal.logging.InternalLogger;

public class BenchUtils {

    private BenchUtils() {
    }

    public static void logEnterRatioIfNeeded(InternalLogger logger) {
        final long total = BatchFlushEndPointContext.HasOngoingSendLoop.ENTERED_FREQUENCY.get()
                + BatchFlushEndPointContext.HasOngoingSendLoop.ENTER_FAILED_FREQUENCY.get();
        if (total == 0L) {
            return;
        }
        logger.info("enter ratio: {}%oo",
                BatchFlushEndPointContext.HasOngoingSendLoop.ENTERED_FREQUENCY.get() * 10000 / (double) total);

    }

    public static void logAvgBatchCount(InternalLogger logger) {
        final long flushedCommandCount = DefaultBatchFlushEndpoint.FLUSHED_COMMAND_COUNT.get();
        final long flushedBatchCount = DefaultBatchFlushEndpoint.FLUSHED_BATCH_COUNT.get();
        if (flushedBatchCount == 0L) {
            logger.info("no batch flushed");
            return;
        }
        logger.info("avg batch count: {}", flushedCommandCount / (double) flushedBatchCount);
    }

}
