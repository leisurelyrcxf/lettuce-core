package io.lettuce.bench;

import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Structure;
import io.lettuce.core.resource.EventLoopGroupProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.lettuce.core.resource.PromiseAdapter.toBooleanPromise;

public class CustomEventLoopGroupProvider implements EventLoopGroupProvider {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(CustomEventLoopGroupProvider.class);

    private static final int NUM_CPU = Runtime.getRuntime().availableProcessors();

    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    private final EventLoopGroup eventLoopGroup;

    public CustomEventLoopGroupProvider() {
        AtomicInteger cpuIdx = new AtomicInteger(0);
        if (Epoll.isAvailable()) {
            this.eventLoopGroup = new EpollEventLoopGroup(4, r -> {
                Thread thread = new Thread(new WrappedRunnable(r, cpuIdx.get() % NUM_CPU));
                cpuIdx.incrementAndGet();
                return thread;
            });
        } else {
            this.eventLoopGroup = new NioEventLoopGroup(4, r -> {
                Thread thread = new Thread(new WrappedRunnable(r, cpuIdx.get() % NUM_CPU));
                cpuIdx.incrementAndGet();
                return thread;
            });
        }
    }

    @Override
    public <T extends EventLoopGroup> T allocate(Class<T> type) {
        return (T) this.eventLoopGroup;
    }

    @Override
    public int threadPoolSize() {
        return ((NioEventLoopGroup) this.eventLoopGroup).executorCount();
    }

    @Override
    public Future<Boolean> release(EventExecutorGroup eventLoopGroup, long quietPeriod, long timeout, TimeUnit unit) {
        return toBooleanPromise(eventLoopGroup.shutdownGracefully(quietPeriod, timeout, unit));
    }

    @Override
    public Future<Boolean> shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
        return release(eventLoopGroup, quietPeriod, timeout, timeUnit);
    }

    // 定义 CPU 集合
    public static class cpu_set_t extends Structure {

        public static final int CPU_SETSIZE = 1024;

        public static final int __NCPUBITS = 8 * NativeLong.SIZE;

        public NativeLong[] __bits = new NativeLong[CPU_SETSIZE / __NCPUBITS];

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("__bits");
        }

        public int size() {
            return __bits.length * NativeLong.SIZE;
        }

    }

    public static void CPU_ZERO(cpu_set_t set) {
        Arrays.fill(set.__bits, new NativeLong(0));
    }

    public static void CPU_SET(int cpu, cpu_set_t set) {
        set.__bits[cpu / cpu_set_t.__NCPUBITS]
                .setValue(set.__bits[cpu / cpu_set_t.__NCPUBITS].longValue() | (1L << (cpu % cpu_set_t.__NCPUBITS)));
    }

    public static native int sched_setaffinity(int pid, int size, cpu_set_t mask);

    private static class WrappedRunnable implements Runnable {

        private final Runnable r;

        private final int cpu;

        public WrappedRunnable(Runnable r, int cpuIdx) {
            this.r = r;
            this.cpu = cpuIdx;
        }

        @Override
        public void run() {
            // 在这里设置线程的 CPU 亲和性
            setCpuAffinity(); // 将线程绑定到 CPU 0
            r.run();
        }

        private void setCpuAffinity() {
            // 使用 JNA 设置 CPU 亲和性
            int tid = getTid();
            if (tid != -1) {
                cpu_set_t cpuset = new cpu_set_t();
                CPU_ZERO(cpuset);
                CPU_SET(cpu, cpuset);
                int result = sched_setaffinity(tid, cpuset.size(), cpuset);
                if (result != 0) {
                    logger.error("Failed to set CPU affinity for thread {} to CPU {}: {}", tid, cpu, Native.getLastError());
                } else {
                    logger.info("Set CPU affinity for thread " + tid + " to CPU " + cpu);
                }
            } else {
                logger.error("Failed to get thread ID");
            }
        }

        private int getTid() {
            // 获取线程 ID，使用 JNA 调用 syscall(SYS_gettid)
            int SYS_gettid = 186; // 在 x86_64 Linux 系统上，SYS_gettid 的系统调用号是 186
            return (int) NativeLibrary.getInstance("c").getFunction("syscall").invokeLong(new Object[] { SYS_gettid });
        }

    }

}
