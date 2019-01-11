package org.corfudb.universe.scenario;

import com.google.common.base.Functions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForClusterDown;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;
import static org.junit.Assert.fail;

@Slf4j
public abstract class AllNodesBaseIT extends GenericIntegrationTest {

    static final int DEFAULT_AMOUNT_OF_NODES = 3;
    static final int QUORUM_AMOUNT_OF_NODES = (DEFAULT_AMOUNT_OF_NODES / 2) + 1;
    static final boolean FAIL_EAGER = false;
    static final int DEFAULT_CLUSTER_REPORT_POLL_ITER = 10;
    static final Duration DELAY_BETWEEN_SEQUENTIAL_FAILURES = Duration.ofMillis(1200);
    static final int MAXIMUN_UNEXPECTED_EXCEPTIONS = 10;
    static final int MAX_FAILURES_RECOVER_RETRIES = 5;

    protected abstract FailureType getFailureType(int corfuServerIndex);

    protected abstract boolean useOneUniversePerTest();

    protected void testAllNodesAllRecoverCombinations(boolean startServersSequentially, int amountUp){
        testAllNodesAllRecoverCombinations(startServersSequentially, amountUp, true);
    }

    protected void testAllNodesAllRecoverCombinations(boolean startServersSequentially, int amountUp,
                                                      boolean permuteCombinations){
        if(useOneUniversePerTest()){
            testAllNodesAllRecoverCombinationsInOneUniverse(startServersSequentially, amountUp, permuteCombinations);
        }else{
            testAllNodesAllRecoverCombinationsInDifferentUniverses(startServersSequentially, amountUp, permuteCombinations);
        }
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified amount is fix afterwards.
     * One universe is created for each test case. A test case is a specific combination-permutation
     * of recovered servers.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private void testAllNodesAllRecoverCombinationsInDifferentUniverses(boolean startServersSequentially, int amountUp,
                                                                        boolean permuteCombinations) {
        ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected = new ArrayList<>();
        if (amountUp >= QUORUM_AMOUNT_OF_NODES) {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.STABLE);
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.DEGRADED);
        } else {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
        }

        ArrayList<CombinationResult> testResult = new ArrayList();
        ArrayList<ArrayList<Integer>> combinationsAndPermutations = combine(DEFAULT_AMOUNT_OF_NODES, amountUp, permuteCombinations);

        ClientParams clientParams = ClientParams.builder()
                .systemDownHandlerTriggerLimit(10)
                .requestTimeout(Duration.ofSeconds(5))
                .idleConnectionTimeout(30)
                .connectionTimeout(Duration.ofMillis(500))
                .connectionRetryRate(Duration.ofMillis(1000))
                .build();

        for (ArrayList<Integer> combination : combinationsAndPermutations) {
            getScenario().describe((fixture, testCase) -> {
                CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

                CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(clientParams);

                CorfuTable<String, String> table = initializeCorfuTable(corfuClient);

                ArrayList<CorfuServer> nodes = new ArrayList<>(corfuCluster.<CorfuServer>nodes().values());

                String combinationName = combination.stream().map(i -> i.toString()).
                        collect(Collectors.joining(" - "));
                testCase.it(getTestCaseDescription(startServersSequentially, combinationName), data -> {
                    CombinationResult result = testAllNodesWithOneRecoverCombination(combination, combinationName, clusterStatusesExpected,
                            (amountUp < QUORUM_AMOUNT_OF_NODES), startServersSequentially, nodes, corfuClient, table);
                    testResult.add(result);
                });
            });
            universe.shutdown();
        }
        executeFinalAssertations(testResult, combinationsAndPermutations);
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified amount is fix afterwards.
     * One single universe is re-used for all test cases. A test case is a specific combination-permutation
     * of recovered servers.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private void testAllNodesAllRecoverCombinationsInOneUniverse(boolean startServersSequentially, int amountUp, boolean permuteCombinations) {
        ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected = new ArrayList<>();
        if (amountUp >= QUORUM_AMOUNT_OF_NODES) {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.STABLE);
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.DEGRADED);
        } else {
            clusterStatusesExpected.add(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
        }

        ArrayList<CombinationResult> testResult = new ArrayList();
        ArrayList<ArrayList<Integer>> combinationsAndPermutations = combine(DEFAULT_AMOUNT_OF_NODES, amountUp, permuteCombinations);

        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            ClientParams clientParams = ClientParams.builder()
                    .systemDownHandlerTriggerLimit(10)
                    .requestTimeout(Duration.ofSeconds(5))
                    .idleConnectionTimeout(30)
                    .connectionTimeout(Duration.ofMillis(500))
                    .connectionRetryRate(Duration.ofMillis(1000))
                    .build();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(clientParams);

            CorfuTable<String, String> table = initializeCorfuTable(corfuClient);

            ArrayList<CorfuServer> nodes = new ArrayList<>(corfuCluster.<CorfuServer>nodes().values());

            for (ArrayList<Integer> combination : combinationsAndPermutations) {
                String combinationName = combination.stream().map(i -> i.toString()).
                        collect(Collectors.joining(" - "));
                testCase.it(getTestCaseDescription(startServersSequentially, combinationName), data -> {
                    CombinationResult result = testAllNodesWithOneRecoverCombination(combination, combinationName, clusterStatusesExpected,
                            (amountUp < QUORUM_AMOUNT_OF_NODES), startServersSequentially, nodes, corfuClient, table);
                    testResult.add(result);
                });
            }
        });
        executeFinalAssertations(testResult, combinationsAndPermutations);
    }

    /**
     * Test cluster behavior after all nodes are are failed and just the specified combination is fix afterwards
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop all nodes
     * 3) Verify cluster status is unavailable, node status are down and data path is not available
     * 4) Restart just one server
     * 5) Wait for the new layout is available
     * 6) Verify the amount of active servers, the cluster status is STABLE or DEGRADED and the data path
     * operations works
     */
    private CombinationResult testAllNodesWithOneRecoverCombination(ArrayList<Integer> combination, String combinationName,
                                                                    ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected,
                                                                    boolean dataPathExceptionExpected, boolean startServersSequentially,
                                                                    ArrayList<CorfuServer> nodes, CorfuClient corfuClient,
                                                                    CorfuTable<String, String> table) {
        CombinationResult result = new CombinationResult(combinationName, combination, clusterStatusesExpected,
                nodes.stream().map(CorfuServer::getEndpoint).collect(Collectors.toList()), table);
        // Force failure in all nodes sequentially without wait to much for it
        if(executeFailureInAllNodes(nodes)) {
            // Verify cluster status is UNAVAILABLE with all nodes NA and UNRESPONSIVE
            final Duration sleepDuration = Duration.ofSeconds(1);
            Sleep.sleepUninterruptibly(sleepDuration);
            log.info(String.format("Verify cluster status after failure in all nodes for test-combination %s", combination));
            ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            for (int i = 0; i < DEFAULT_CLUSTER_REPORT_POLL_ITER; i++) {
                if (clusterStatusReport.getClusterStatus() != ClusterStatusReport.ClusterStatus.UNAVAILABLE) {
                    log.info(String.format("Cluster status after failure in all nodes: %s", clusterStatusReport.getClusterStatus()));
                    Sleep.sleepUninterruptibly(sleepDuration);
                    clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                } else
                    break;
            }
            Map<String, ClusterStatusReport.ConnectivityStatus> connectionMap = clusterStatusReport.getClientServerConnectivityStatusMap();
            Map<String, ClusterStatusReport.NodeStatus> statusMap = clusterStatusReport.getClusterNodeStatusMap();
            log.info(String.format("Connection map after failure in all nodes: %s", connectionMap));
            log.info(String.format("Status map after failure in all nodes: %s", statusMap));
            int nodesStoped = 0;
            for (int i = 0; i < nodes.size(); i++) {
                CorfuServer s = nodes.get(i);
                switch (getFailureType(i)){
                    case STOP_NODE:
                        assertThat(connectionMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE);
                        assertThat(statusMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.NodeStatus.NA);
                        nodesStoped++;
                        break;
                    case DISCONNECT_NODE:
                        assertThat(connectionMap.get(s.getEndpoint())).isEqualTo(ClusterStatusReport.ConnectivityStatus.RESPONSIVE);
                        //In this case is not clear which is the expected Node Status
                        break;
                    case NONE:
                        break;
                }
            }
            if(nodesStoped >= QUORUM_AMOUNT_OF_NODES){
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
            }
            Layout afterStopLayout = clusterStatusReport.getLayout();
            log.info(String.format("Layout after all servers down: %s", afterStopLayout));

            //Verify data path
            // At this point, if the cluster is completely partitioned, the local corfu client is still capable
            // to communicate with every node.
            // It's not clear if under this conditions the data path operation should fail or should works.
            // That's why the implementation is check if we are able to write, otherwise dono not check any thing
            executeIntermediateCorfuTableWrites(result);

            // Start the combination of nodes
            log.info(String.format("Fix nodes for combination %s", combination));
            ExecutorService executor;
            if (startServersSequentially) {
                executor = null;
                combination.forEach(i -> {
                    CorfuServer corfuServer = nodes.get(i);
                    boolean recoverRes = recoverServer(corfuServer, i, "Error executing fix for failure (%s)",
                            "Imposible to execute fix");
                    result.setImposibleToExecuteFixWithOr(!recoverRes);
                });
            } else {
                executor = Executors.newFixedThreadPool(combination.size());
                combination.forEach(i -> executor.submit(() -> {
                    CorfuServer corfuServer = nodes.get(i);
                    boolean recoverRes = recoverServer(corfuServer, i, "Error executing fix for failure (%s)",
                            "Imposible to execute fix");
                    result.setImposibleToExecuteFixWithOr(!recoverRes);
                }));
            }

            // Verify cluster status is the expected
            log.info(String.format("Verify cluster status after fix the nodes combination %s", combination));
            for (int i = 0; i < DEFAULT_CLUSTER_REPORT_POLL_ITER; i++) {
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                if (!clusterStatusesExpected.contains(clusterStatusReport.getClusterStatus())) {
                    log.info(String.format("Cluster status: %s", clusterStatusReport.getClusterStatus()));
                    Sleep.sleepUninterruptibly(sleepDuration);
                } else
                    break;
            }

            log.info(String.format("Status for combination %s", combination));
            Layout afterStartLayout = clusterStatusReport.getLayout();
            log.info(String.format("Layout after servers up: %s", afterStartLayout));
            if (afterStartLayout != null) {
                result.activeServers = afterStartLayout.getAllActiveServers().size();
                log.info(String.format("Nodes active: %s", result.activeServers));
                log.info(String.format("Unresponsive Nodes: %s", afterStartLayout.getUnresponsiveServers().size()));
            } else {
                log.info("Nodes active: None (layout is null)");
                result.activeServers = 0;
            }
            result.connectionMap = clusterStatusReport.getClientServerConnectivityStatusMap();
            log.info(String.format("Connection map after fix the nodes: %s", result.connectionMap));
            result.statusMap = clusterStatusReport.getClusterNodeStatusMap();
            log.info(String.format("Status map after fix the nodes: %s", result.statusMap));
            result.clusterStatus = clusterStatusReport.getClusterStatus();
            log.info(String.format("Cluster status: %s", result.clusterStatus));
            if (FAIL_EAGER) {
                result.assertResultStatus();
            }

            // Verify data path working fine
            testDataPathGetAfterRecovering(combination, dataPathExceptionExpected, result);
            if (executor != null)
                executor.shutdownNow();
            // Recover nodes not present in the combination
            IntStream.range(0, nodes.size() - 1).filter(i -> !combination.contains(i)).forEach(j -> {
                recoverServer(nodes.get(j), j, "Error executing recovering from (%s)", "Imposible to execute recovering");
            });
        }else{
            result.imposibleToExecuteFailure = true;
        }
        return result;
    }

    private void executeFinalAssertations(ArrayList<CombinationResult> testResult, ArrayList<ArrayList<Integer>> combinationsAndPermutations) {
        int imposibleToExecuteFailureCount = 0;
        int imposibleToExecuteFixCount = 0;
        for (CombinationResult r : testResult) {
            if(!r.imposibleToExecuteFailure && !r.imposibleToExecuteFix) {
                if (!FAIL_EAGER) {
                    r.assertResults();
                }
            }else if(r.imposibleToExecuteFailure){
                imposibleToExecuteFailureCount++;
            }else{
                imposibleToExecuteFixCount++;
            }
        }
        float untestedCombinations = (float) (imposibleToExecuteFailureCount + imposibleToExecuteFixCount);
        float maxUntestedCombinationsAllowed = ((float)combinationsAndPermutations.size()) / 2;
        if(untestedCombinations > 0)
            log.warn(String.format("%s combination has not been tested", untestedCombinations));
        assertThat(untestedCombinations).isLessThan(maxUntestedCombinationsAllowed);
    }

    private String getTestCaseDescription(boolean startServersSequentially, String combinationName){
        String orderType = startServersSequentially ? "sequentially" : "concurrently";
        String description = String.format("Should stop all nodes %s, restart the nodes %s and recover", orderType,
                combinationName);
        return description;
    }

    private boolean executeFailureInAllNodes(ArrayList<CorfuServer> nodes) {
        boolean res = false;
        List<Integer> stoppedNodes = new ArrayList<>();
        for (int j= 0; j < nodes.size(); j++) {
            CorfuServer s = nodes.get(j);
            for (int i = 0; i < MAX_FAILURES_RECOVER_RETRIES; i++) {
                Sleep.sleepUninterruptibly(DELAY_BETWEEN_SEQUENTIAL_FAILURES);
                try {
                    switch (getFailureType(j)) {
                        case STOP_NODE:
                            s.stop(Duration.ofNanos(1));
                            stoppedNodes.add(j);
                            break;
                        case DISCONNECT_NODE:
                            List<CorfuServer> noStoppedNodes = new ArrayList<>();
                            for (int k = 0; k < nodes.size(); k++) {
                                if(!stoppedNodes.contains(k))
                                    noStoppedNodes.add(nodes.get(k));
                            }
                            s.disconnect(noStoppedNodes);
                            break;
                        case NONE:
                            break;
                    }
                    res = true;
                    break;
                }catch (Exception ex){
                    log.error(String.format("Error executing failure (%s)", getFailureType(j)), ex);
                    res = false;
                }
            }
            if(!res)
                break;
        }
        return res;
    }

    private CorfuTable<String, String> initializeCorfuTable(CorfuClient corfuClient) {
        CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME);
        for (int i = 0; i < Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
            table.put(String.valueOf(i), String.valueOf(i));
        }
        return table;
    }

    private void executeIntermediateCorfuTableWrites(CombinationResult result) {
        for (int i = 0; i < 5; i++) {
            try {
                result.table.put(getIntermediateKey(result, i), getIntermediateValue(i));
                result.lastIntermediateWrite = i;
            }catch (Exception ex){
                log.warn(String.format("Exception writing intermediate data value %s: %s", i,
                        ex));
                break;
            }
        }
    }

    private String getIntermediateValue(int index) {
        return String.format("IntermediateValue%s", index);
    }

    private String getIntermediateKey(CombinationResult result, int index) {
        return String.format("%s-IntermediateKey-%s", result.combinationName, index);
    }

    private void testDataPathGetAfterRecovering(ArrayList<Integer> combination, boolean dataPathExceptionExpected, CombinationResult result) {
        log.info(String.format("Verify data path for combination %s", combination));
        int unexpectedExceptions = 0;
        for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            try {
                if (dataPathExceptionExpected) {
                    String unexpectedValue = result.table.get(String.valueOf(i));
                    String mess = String.format("Expected an UnreachableClusterException to be thrown but %s obtained",
                            unexpectedValue);
                    if (FAIL_EAGER) {
                        fail(mess);
                    } else {
                        log.info(mess);
                        result.dataStatus.add(false);
                    }
                } else {
                    if (FAIL_EAGER) {
                        assertThat(result.table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                    } else {
                        boolean dataStatus = result.table.get(String.valueOf(i)).equals(String.valueOf(i));
                        if (!dataStatus)
                            log.info(String.format("Incorrect value for %s", i));
                        result.dataStatus.add(dataStatus);
                    }
                }

            } catch (Exception ex) {
                if (dataPathExceptionExpected) {
                    log.info(String.format("Exception checking data value %s: %s", i,
                            ex));
                    if (FAIL_EAGER) {
                        assertThat(ex.getMessage()).startsWith("Cluster is unavailable");
                    } else {
                        result.dataStatus.add(ex.getMessage().startsWith("Cluster is unavailable"));
                        break;
                    }
                } else {
                    unexpectedExceptions++;
                    log.info(String.format("Unexpected exception checking data value %s: %s", i,
                            ex));
                    result.dataStatus.add(false);
                    if(unexpectedExceptions >= MAXIMUN_UNEXPECTED_EXCEPTIONS)
                        break;
                }
            }
        }
        if(!dataPathExceptionExpected && result.lastIntermediateWrite >= 0){
            for (int i = 0; i < result.lastIntermediateWrite; i++) {
                try{
                    boolean dataStatus = result.table.get(getIntermediateKey(result, i)).equals(getIntermediateValue(i));
                    if (!dataStatus) {
                        String mess = String.format("Incorrect intermediate value for %s", i);
                        if (FAIL_EAGER){
                            fail();
                        }else{
                            log.info(mess);
                        }
                    }
                    result.dataStatus.add(dataStatus);
                }catch (Exception ex){
                    unexpectedExceptions++;
                    log.info(String.format("Unexpected exception checking intermediate data value %s: %s", i,
                            ex));
                    result.dataStatus.add(false);
                    if(unexpectedExceptions >= MAXIMUN_UNEXPECTED_EXCEPTIONS)
                        break;
                }
            }
        }
    }

    private boolean recoverServer(CorfuServer corfuServer, int corfuServerIndex, String s, String s2) {
        boolean res = false;
        for (int j = 0; j < MAX_FAILURES_RECOVER_RETRIES; j++) {
            Sleep.sleepUninterruptibly(DELAY_BETWEEN_SEQUENTIAL_FAILURES);
            try {
                switch (getFailureType(corfuServerIndex)) {
                    case STOP_NODE:
                        corfuServer.start();
                        break;
                    case DISCONNECT_NODE:
                        corfuServer.reconnect();
                        break;
                    case NONE:
                        break;
                }
                res = true;
                break;
            } catch (Exception ex) {
                log.error(String.format(s, getFailureType(corfuServerIndex)), ex);
                res = false;
            }
        }
        return res;
    }

    /**
     * Generate all the combinations  of k numbers with its respective permutations, of n numbers.
     *
     * @param n numbers tu combine
     * @param k amount of numbers per combination
     * @return n in k combinations with its respective permutations
     */
    private ArrayList<ArrayList<Integer>> combine(int n, int k, boolean permuteCombinations) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
        if (n <= 0 || n < k)
            return result;
        ArrayList<Integer> item = new ArrayList<Integer>();
        dfs(n, k, 0, item, result, permuteCombinations);
        return result;
    }

    /**
     * Recursively generates all combinations of k numbers with its respective permutations, of n numbers.
     *
     * @param n     numbers tu combine
     * @param k     amount of numbers per combination
     * @param start number used to start the recursion
     * @param item  current combination to fill
     * @param res   n in k combinations with its respective permutations
     */
    private void dfs(int n, int k, int start, ArrayList<Integer> item, ArrayList<ArrayList<Integer>> res,
                     boolean permuteCombinations) {
        if (item.size() == k) {
            if(permuteCombinations)
                permute(0, item, res);
            else
                res.add(new ArrayList<Integer>(item));
            return;
        }
        for (int i = start; i < n; i++) {
            item.add(i);
            dfs(n, k, i + 1, item, res, permuteCombinations);
            item.remove(item.size() - 1);
        }
    }

    /**
     * Permute the number indicate with the others in the list.
     *
     * @param i      position of the number to permute
     * @param nums   list of numbers
     * @param result the list were all permutations are added
     */
    private void permute(int i, ArrayList<Integer> nums, ArrayList<ArrayList<Integer>> result) {
        if (i == nums.size() - 1) {
            result.add(new ArrayList<Integer>(nums));
            return;
        }

        for (int j = i; j < nums.size(); j++) {
            swap(nums, i, j);
            permute(i + 1, nums, result);
            swap(nums, i, j);
        }
    }

    /**
     * Swap the numbers in the positions specified.
     *
     * @param nums list of numbers
     * @param i    position of the first number to swap
     * @param j    position of the second number to swap
     */
    private void swap(ArrayList<Integer> nums, int i, int j) {
        int t = nums.get(i);
        nums.set(i, nums.get(j));
        nums.set(j, t);
    }

    /**
     * Structure that holds parameters and results of a combination of server
     * to heal after servers failures
     */
    @Data
    private class CombinationResult {
        //Parameters
        public final String combinationName;
        public final ArrayList<Integer> combination;
        public final ArrayList<ClusterStatusReport.ClusterStatus> clusterStatusesExpected;
        public final List<String> endpoints;
        public final CorfuTable<String, String> table;
        public int lastIntermediateWrite = -1;

        //Results
        public boolean imposibleToExecuteFailure = false;
        public boolean imposibleToExecuteFix = false;
        public int activeServers = 0;
        public ClusterStatusReport.ClusterStatus clusterStatus = ClusterStatusReport.ClusterStatus.UNAVAILABLE;
        public Map<String, ClusterStatusReport.ConnectivityStatus> connectionMap = null;
        public Map<String, ClusterStatusReport.NodeStatus> statusMap = null;
        public ArrayList<Boolean> dataStatus = new ArrayList<>();

        public synchronized void setImposibleToExecuteFixWithOr(boolean valueToOr){
            imposibleToExecuteFix = (valueToOr || imposibleToExecuteFix);
        }

        public void assertResultStatus() {
            assertThat(clusterStatus).isIn(clusterStatusesExpected);
            combination.forEach(i -> assertThat(connectionMap.get(endpoints.get(i))).isEqualTo(ClusterStatusReport.ConnectivityStatus.RESPONSIVE));
            combination.forEach(i -> assertThat(statusMap.get(endpoints.get(i))).isEqualTo(ClusterStatusReport.NodeStatus.UP));
            assertThat(activeServers).isEqualTo(combination.size());
        }

        public void assertResults() {
            dataStatus.forEach(s -> assertThat(s).isTrue());
            assertResultStatus();
        }
    }

    protected enum FailureType {
        STOP_NODE,
        DISCONNECT_NODE,
        NONE
    }
}