package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IDataStore;
import org.corfudb.infrastructure.IDataStore.KvRecord;
import org.corfudb.runtime.view.Address;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data access layer for StreamLog
 */
@Builder
@Slf4j
public class StreamLogDataStore {
    public static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>("TAIL_SEGMENT", "CURRENT", Long.class);

    public static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            "STARTING_ADDRESS", "CURRENT", Long.class
    );

    private static final long ZERO_ADDRESS = 0L;

    @NonNull
    private final IDataStore dataStore;

    /**
     * Cached starting address
     */
    private final AtomicLong startingAddress = new AtomicLong(Address.NON_ADDRESS);
    /**
     * Cached tail segment
     */
    private final AtomicLong tailSegment = new AtomicLong(Address.NON_ADDRESS);

    /**
     * Return current cached tail segment or get the segment from the data store if not initialized
     * @return tail segment
     */
    public long getTailSegment() {
        if (tailSegment.get() == Address.NON_ADDRESS) {
            long dbTailSegment = Optional.ofNullable(dataStore.get(TAIL_SEGMENT_RECORD)).orElse(0L);
            tailSegment.set(dbTailSegment);
        }

        return tailSegment.get();
    }

    /**
     * Update current tail segment in the data store
     * @param newTailSegment updated tail segment
     */
    public void updateTailSegment(long newTailSegment) {
        if (tailSegment.get() >= newTailSegment) {
            log.debug("New tail segment equals to the old one: {}. Ignore", newTailSegment);
            return;
        }

        dataStore.put(newTailSegment, TAIL_SEGMENT_RECORD);
        tailSegment.set(newTailSegment);
    }

    /**
     * Returns the dataStore starting address.
     *
     * @return the starting address
     */
    public long getStartingAddress() {
        if (startingAddress.get() == Address.NON_ADDRESS) {
            long dbStartingAddress = Optional.ofNullable(dataStore.get(STARTING_ADDRESS_RECORD)).orElse(ZERO_ADDRESS);
            startingAddress.set(dbStartingAddress);
        }

        return startingAddress.get();
    }

    /**
     * Update current starting address in the data store
     * @param newStartingAddress updated starting address
     */
    public void updateStartingAddress(long newStartingAddress) {
        dataStore.put(newStartingAddress, STARTING_ADDRESS_RECORD);
        startingAddress.set(newStartingAddress);
    }

    /**
     * Reset tail segment
     */
    public void resetTailSegment() {
        log.info("Reset tail segment. Current segment: {}", tailSegment.get());
        dataStore.put(ZERO_ADDRESS, TAIL_SEGMENT_RECORD);
        tailSegment.set(ZERO_ADDRESS);
    }

    /**
     * Reset starting address
     */
    public void resetStartingAddress() {
        log.info("Reset starting address. Current address: {}", startingAddress.get());
        dataStore.put(ZERO_ADDRESS, STARTING_ADDRESS_RECORD);
        startingAddress.set(ZERO_ADDRESS);
    }
}
