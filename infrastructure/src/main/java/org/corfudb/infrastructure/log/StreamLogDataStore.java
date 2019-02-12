package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.infrastructure.IDataStore;
import org.corfudb.infrastructure.IDataStore.KvRecord;

import java.util.Optional;

/**
 * Data access layer for StreamLog
 */
@Builder
public class StreamLogDataStore {
    public static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>("TAIL_SEGMENT", "CURRENT", Long.class);

    public static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            "STARTING_ADDRESS", "CURRENT", Long.class
    );

    @NonNull
    private final IDataStore dataStore;

    public long getTailSegment() {
        return Optional.ofNullable(dataStore.get(TAIL_SEGMENT_RECORD)).orElse(0L);
    }

    public void saveTailSegment(long tailSegment) {
        dataStore.put(tailSegment, TAIL_SEGMENT_RECORD);
    }

    /**
     * Returns the dataStore starting address.
     *
     * @return the starting address
     */
    public long getStartingAddress() {
        return Optional.ofNullable(dataStore.get(STARTING_ADDRESS_RECORD)).orElse(0L);
    }

    public void saveStartingAddress(long startingAddress) {
        dataStore.put(startingAddress, STARTING_ADDRESS_RECORD);
    }
}
