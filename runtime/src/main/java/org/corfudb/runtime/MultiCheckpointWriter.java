package org.corfudb.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

/**
 * Checkpoint multiple SMRMaps serially as a prerequisite for a later log trim.
 */
@Slf4j
public class MultiCheckpointWriter<T extends Map> {
    @Getter
    private List<ICorfuSMR<T>> maps = new ArrayList<>();

    /** Add a map to the list of maps to be checkpointed by this class. */
    @SuppressWarnings("unchecked")
    public void addMap(T map) {
        maps.add((ICorfuSMR<T>) map);
    }

    /** Add map(s) to the list of maps to be checkpointed by this class. */

    public void addAllMaps(Collection<T> maps) {
        for (T map : maps) {
            addMap(map);
        }
    }


    /** Checkpoint multiple SMRMaps. Since this method is Map specific
     *  then the keys are unique and the order doesn't matter.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     */

    public Token appendCheckpoints(CorfuRuntime rt, String author) {
        log.info("appendCheckpoints: appending checkpoints for {} maps", maps.size());

        // TODO(Maithem) should we throw an exception if a new min is not discovered
        Token minSnapshot = Token.UNINITIALIZED;

        final long cpStart = System.currentTimeMillis();
        try {
            for (ICorfuSMR<T> map : maps) {
                UUID streamId = map.getCorfuStreamID();
                final long mapCpStart = System.currentTimeMillis();
                int mapSize = ((T) map).size();
                CheckpointWriter<T> cpw = new CheckpointWriter(rt, streamId, author, (T) map);
                ISerializer serializer =
                        ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                                .getSerializer();
                cpw.setSerializer(serializer);
                log.trace("appendCheckpoints: checkpoint map {} begin",
                        Utils.toReadableId(map.getCorfuStreamID()));
                Token minCPSnapshot = cpw.appendCheckpoint();

                if (minSnapshot == Token.UNINITIALIZED) {
                    minSnapshot = minCPSnapshot;
                } else if (minSnapshot.getEpoch() != minCPSnapshot.getEpoch()) {
                    //TODO(Maithem): add epochs?
                    throw new IllegalStateException("Epoch changed during GC cycle aborting");
                } else if (Token.min(minCPSnapshot, minSnapshot) == minCPSnapshot) {
                    // Adopt the new ming
                    minSnapshot = minCPSnapshot;
                }

                log.trace("appendCheckpoints: checkpoint map {} end",
                        Utils.toReadableId(map.getCorfuStreamID()));

                final long mapCpEnd = System.currentTimeMillis();

                log.info("appendCheckpoints: took {} ms to checkpoint {} entries for {}",
                        mapCpEnd - mapCpStart, mapSize, streamId);
            }
        } finally {
            // TODO(Maithem): print cp id?
            log.trace("appendCheckpoints: finished, author '{}' at min globalAddress {}",
                    author, minSnapshot);
            rt.getObjectsView().TXEnd();
        }
        final long cpStop = System.currentTimeMillis();

        log.info("appendCheckpoints: took {} ms to append {} checkpoints", cpStop - cpStart,
                maps.size());
        return minSnapshot;
    }

}
