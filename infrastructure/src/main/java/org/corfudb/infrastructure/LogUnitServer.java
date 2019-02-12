package org.corfudb.infrastructure;

import static org.corfudb.infrastructure.ServerThreadFactory.ExceptionHandler;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FillHoleRequest;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Created by mwei on 12/10/15.
 *
 * <p>A Log Unit Server, which is responsible for providing the persistent storage for the Corfu
 * Distributed Shared Log.
 *
 * <p>All reads and writes go through a cache. For persistence, every 10,000 log entries are written
 * to individual files (logs), which are represented as FileHandles. Each FileHandle contains a
 * pointer to the tail of the file, a memory-mapped file channel, and a set of addresses known
 * to be in the file. To append an entry, the pointer to the tail is first extended to the
 * length of the entry, and the entry is added to the set of known addresses. A header is
 * written, which consists of the ASCII characters LE, followed by a set of flags, the log unit
 * address, the size of the entry, then the metadata size, metadata and finally the entry itself
 * . When the entry is complete, a written flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    /**
     * The options map.
     */
    private final LogUnitServerConfig config;

    /**
     * The server context of the node.
     */
    private final ServerContext serverContext;

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler = CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent
     * storage.
     */
    private final AtomicReference<LoadingCache<Long, ILogData>> dataCache = new AtomicReference<>();
    private final AtomicReference<StreamLog> streamLog = new AtomicReference<>();
    private final AtomicReference<BatchWriter<Long, ILogData>> batchWriter = new AtomicReference<>();

    private final StreamLogCompaction logCleaner;

    private final ExecutorService executor;

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Returns a new LogUnitServer.
     *
     * @param serverContext context object providing settings and objects
     */
    public LogUnitServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.config = LogUnitServerConfig.parse(serverContext.getServerConfig());

        executor = Executors.newFixedThreadPool(
                serverContext.getLogunitThreadCount(),
                new ServerThreadFactory("LogUnit-", new ExceptionHandler())
        );

        init(serverContext);

        logCleaner = new StreamLogCompaction(streamLog.get(), 10, 45, TimeUnit.MINUTES, ServerContext.SHUTDOWN_TIMER);
    }

    private synchronized void init(ServerContext serverContext) {
        if (config.isMemoryMode()) {
            String warnMessage = "Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.";

            log.warn(warnMessage, FileUtils.byteCountToDisplaySize(config.getMaxCacheSize()));
            streamLog.set(new InMemoryStreamLog());
        } else {
            streamLog.set(new StreamLogFiles(serverContext, config.isNoVerify()));
        }

        batchWriter.set(new BatchWriter<>(streamLog.get(), serverContext.getServerEpoch(), !config.isNoSync()));

        LoadingCache<Long, ILogData> cache = Caffeine.newBuilder()
                .weigher((k, v) -> {
                    byte[] logData = ((LogData) v).getData();
                    return logData == null ? 1 : logData.length;
                })
                .maximumWeight(config.getMaxCacheSize())
                .writer(batchWriter.get())
                .build(this::handleRetrieval);

        dataCache.set(cache);
    }

    /**
     * Service an incoming request for maximum global address the log unit server has written.
     */
    @ServerHandler(type = CorfuMsgType.TAIL_REQUEST)
    public void handleTailRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        TailsResponse tails = batchWriter.get().queryTails(msg.getEpoch());
        r.sendResponse(ctx, msg, CorfuMsgType.TAIL_RESPONSE.payloadMsg(tails));
    }

    /**
     * Service an incoming request to retrieve the starting address of this logging unit.
     */
    @ServerHandler(type = CorfuMsgType.TRIM_MARK_REQUEST)
    public void handleHeadRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.TRIM_MARK_RESPONSE.payloadMsg(streamLog.get().getTrimMark()));
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE)
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log write: global: {}, streams: {}", msg.getPayload().getToken(),
                msg.getPayload().getData().getBackpointerMap());

        try {
            LogData logData = (LogData) msg.getPayload().getData();
            logData.setEpoch(msg.getEpoch());
            dataCache.get().put(msg.getPayload().getGlobalAddress(), logData);
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.payloadMsg(ex.getOverWriteCause().getId()));
        } catch (DataOutrankedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } catch (ValueAdoptedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_VALUE_ADOPTED.payloadMsg(e
                    .getReadResponse()));
        }
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST)
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("read: {}", msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        try {
            for (Long l = msg.getPayload().getRange().lowerEndpoint();
                 l < msg.getPayload().getRange().upperEndpoint() + 1L; l++) {
                ILogData e = dataCache.get().get(l);
                if (e == null) {
                    rr.put(l, LogData.getEmpty(l));
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.MULTIPLE_READ_REQUEST)
    private void multiRead(CorfuPayloadMsg<MultipleReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("multiRead: {}", msg.getPayload().getAddresses());

        ReadResponse rr = new ReadResponse();
        try {
            for (Long l : msg.getPayload().getAddresses()) {
                ILogData e = dataCache.get().get(l);
                if (e == null) {
                    rr.put(l, LogData.getEmpty(l));
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.FILL_HOLE)
    private void fillHole(CorfuPayloadMsg<FillHoleRequest> msg, ChannelHandlerContext ctx,
                          IServerRouter r) {
        try {
            Token address = msg.getPayload().getAddress();
            log.debug("fillHole: filling address {}, epoch {}", address, msg.getEpoch());
            LogData hole = LogData.getHole(address.getSequence());
            hole.setEpoch(msg.getEpoch());
            dataCache.get().put(address.getSequence(), hole);
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.payloadMsg(e.getOverWriteCause().getId()));
        } catch (DataOutrankedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } catch (ValueAdoptedException e) {

            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_VALUE_ADOPTED.payloadMsg(e
                    .getReadResponse()));
        }
    }

    @ServerHandler(type = CorfuMsgType.PREFIX_TRIM)
    private void prefixTrim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx,
                            IServerRouter r) {
        try {
            TrimRequest req = msg.getPayload();
            batchWriter.get().prefixTrim(req.getAddress());
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } catch (TrimmedException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_TRIMMED.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.COMPACT_REQUEST)
    private void compact(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        try {
            streamLog.get().compact();
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } catch (RuntimeException ex) {
            log.error("Internal Error");
            //TODO(Maithem) Need an internal error return type
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.FLUSH_CACHE)
    private void flushCache(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        try {
            dataCache.get().invalidateAll();
        } catch (RuntimeException e) {
            log.error("Encountered error while flushing cache {}", e);
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }


    /**
     * Services incoming range write calls.
     */
    @ServerHandler(type = CorfuMsgType.RANGE_WRITE)
    private void rangeWrite(CorfuPayloadMsg<RangeWriteMsg> msg,
                            ChannelHandlerContext ctx, IServerRouter r) {
        List<LogData> entries = msg.getPayload().getEntries();
        batchWriter.get().bulkWrite(entries, msg.getEpoch());
        r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
    }

    /**
     * Seal the server with the epoch.
     * <p>
     * - A seal operation is inserted in the queue and then we wait to flush all operations
     * in the queue before this operation.
     * - All operations after this operation but stamped with an older epoch will be failed.
     */
    @Override
    public void sealServerWithEpoch(long epoch) {
        batchWriter.get().waitForSealComplete(epoch);
        log.info("LogUnit sealServerWithEpoch: sealed and flushed with epoch {}", epoch);
    }

    /**
     * Resets the log unit server via the BatchWriter.
     * Warning: Clears all data.
     * <p>
     * - The epochWaterMark is set to prevent resetting log unit multiple times during
     * same epoch.
     * - After this the reset operation is inserted which resets and clears all data.
     * - Finally the cache is invalidated to purge the existing entries.
     */
    @ServerHandler(type = CorfuMsgType.RESET_LOGUNIT)
    private synchronized void resetLogUnit(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        // Check if the reset request is with an epoch greater than the last reset epoch seen to
        // prevent multiple reset in the same epoch. and should be equal to the current router
        // epoch to prevent stale reset requests from wiping out the data.
        if (msg.getPayload() <= serverContext.getLogUnitEpochWaterMark() ||
                msg.getPayload() != serverContext.getServerEpoch()) {
            log.info("LogUnit Server Reset request received but reset already done.");
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
            return;
        }

        serverContext.setLogUnitEpochWaterMark(msg.getPayload());

        batchWriter.get().close();
        streamLog.get().reset();
        init(serverContext);
        log.info("LogUnit Server Reset.");
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * <p>
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and append(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    private synchronized ILogData handleRetrieval(long address) {
        LogData entry = streamLog.get().read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        logCleaner.shutdown();
        batchWriter.get().close();
    }

    @VisibleForTesting
    public LoadingCache<Long, ILogData> getDataCache() {
        return dataCache.get();
    }

    @VisibleForTesting
    long getMaxCacheSize() {
        return config.getMaxCacheSize();
    }

    /**
     * Log unit server configuration class
     */
    @Builder
    @Getter
    public static class LogUnitServerConfig {
        private final double cacheSizeHeapRatio;
        private final long maxCacheSize;
        private final boolean memoryMode;
        private final boolean noVerify;
        private final boolean noSync;

        /**
         * Parse legacy configuration options
         *
         * @param opts legacy config
         * @return log unit configuration
         */
        public static LogUnitServerConfig parse(Map<String, Object> opts) {
            double cacheSizeHeapRatio = Double.parseDouble((String) opts.get("--cache-heap-ratio"));

            return LogUnitServerConfig.builder()
                    .cacheSizeHeapRatio(cacheSizeHeapRatio)
                    .maxCacheSize((long) (Runtime.getRuntime().maxMemory() * cacheSizeHeapRatio))
                    .memoryMode(Boolean.valueOf(opts.get("--memory").toString()))
                    .noVerify((Boolean) opts.get("--no-verify"))
                    .noSync((Boolean) opts.get("--no-sync"))
                    .build();
        }
    }
}
