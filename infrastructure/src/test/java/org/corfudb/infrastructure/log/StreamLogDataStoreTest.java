package org.corfudb.infrastructure.log;

import static org.junit.Assert.*;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.DataStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StreamLogDataStoreTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testGetAndSave() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("--log-path", tempDir.getRoot().getAbsolutePath());

        DataStore ds = new DataStore(opts, val -> log.info("clean up"));

        StreamLogDataStore streamLogDs = StreamLogDataStore.builder()
                .dataStore(ds)
                .build();

        final int tailSegment = 333;
        streamLogDs.saveTailSegment(tailSegment);
        assertEquals(tailSegment, streamLogDs.getTailSegment());

        final int startingAddress = 555;
        streamLogDs.saveStartingAddress(startingAddress);
        assertEquals(startingAddress, streamLogDs.getStartingAddress());
    }
}