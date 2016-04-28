package com.mahsanchez.coherence.datacaching.management;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;

import static com.mahsanchez.coherence.datacaching.management.util.PartitionHelper.MACHINE_SAFE;

public class CoherenceClusterStatusTest {
    private final String CACHE_NAME = "test";
    private final int TOTAL_ENTRIES = 1000;
    private ClusterMemberGroup memberGroup;
    private NamedCache cache = null;
    private int [][] racks = null;

    @Before
    public void setUp() throws Exception {
        memberGroup = ClusterMemberGroupUtils.newBuilder().buildAndConfigureForNoClient();
        final int numberOfRacks = 1;
        final int numberOfMachines = 2;
        final int numberOfStorageEnabledMembersPerMachine = 2;
        final int expectedClusterSize = (numberOfRacks * numberOfMachines * numberOfStorageEnabledMembersPerMachine);

        racks = new int[numberOfRacks][numberOfMachines * numberOfStorageEnabledMembersPerMachine];

        // Start up the storage enabled members on different racks and machines
        for (int rack = 0; rack < numberOfRacks; rack++) {
            for (int machine = 0; machine < numberOfMachines; machine++) {
                // Build using the identity parameters
                memberGroup.merge(ClusterMemberGroupUtils
                        .newBuilder()
                        .setFastStartJoinTimeoutMilliseconds(100)
                        .setSiteName("site1")
                        .setMachineName("r-" + rack + "-m-" + machine)
                        .setRackName("r-" + rack)
                        .setStorageEnabledCount(numberOfStorageEnabledMembersPerMachine)
                        .buildAndConfigureForNoClient());
            }
        }

        // Create Management and client members with default rack and machine identities
        memberGroup.merge(ClusterMemberGroupUtils.newBuilder().buildAndConfigureForStorageDisabledClient());
        assertEquals("Cluster Size", CacheFactory.ensureCluster().getMemberSet().size(), expectedClusterSize + 1);

        //Initialize the Cache
        cache = CacheFactory.getCache(CACHE_NAME);
        Map<Integer, String> entries = new HashMap<>();
        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            entries.put(i, "entry" + i);
        }
        cache.putAll(entries);
        assertEquals("Cache Size", cache.size(), TOTAL_ENTRIES);

        // Pausing to allow partitions to be rebalanced
        TimeUnit.SECONDS.sleep(memberGroup.getSuggestedSleepAfterStopDuration() * 5);
    }

    @After
    public void tearDown() {
        memberGroup.stopAll();
        ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup);
    }

    @Test
    public void testServiceStatusHA() {
        CoherenceClusterStatus client = new CoherenceClusterStatus();
        Map<String, String> services = client.getServicesStatusHA();
        assertEquals("There must be more than one services configured", services.keySet().size(), 1);
        for (String statusHA : services.values()) {
            assertEquals("Services must be in MACHINE-SAFE status", statusHA, MACHINE_SAFE);
        }
    }

    @Test
    public void testServiceStatusHAFailedTimeOut() {
        CoherenceClusterStatus client = new CoherenceClusterStatus();
        Map<String, String> services = client.getServicesStatusHA(1);
        assertEquals("There must be more than one services configured", services.keySet().size(), 0);
    }

    @Test
    public void testServiceStatusHASuccessfullWaitTime() {
        CoherenceClusterStatus client = new CoherenceClusterStatus();
        Map<String, String> services = client.getServicesStatusHA(20000);
        assertEquals("There must be more than one services configured", services.keySet().size(), 1);
        for (String statusHA : services.values()) {
            assertEquals("Services must be in MACHINE-SAFE status", statusHA, MACHINE_SAFE);
        }
    }
}