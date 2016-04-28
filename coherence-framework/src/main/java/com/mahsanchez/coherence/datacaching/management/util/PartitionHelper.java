package com.mahsanchez.coherence.datacaching.management.util;

import java.util.HashMap;
import java.util.Map;

import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Member;
import com.tangosol.net.partition.PartitionAssignmentStrategy;

/**
 * Helper class for Partitions
 */

public class PartitionHelper
{
	public static final String SITE_SAFE = "SITE-SAFE";
	public static final String RACK_SAFE = "RACK-SAFE";
	public static final String MACHINE_SAFE = "MACHINE-SAFE";
	public static final String NODE_SAFE = "NODE-SAFE";
	public static final String ENDANGERED = "ENDANGERED";

	/**
	 * Method description
	 * 
	 * @param cacheService
	 *            to get the StatusHA from
	 * 
	 * @return the StatusHA for the service (MACHINE_SAFE, SITE_SAFE, RACK_SAFE)
	 */
	public static String getStatusHA(DistributedCacheService cacheService)
	{
		PartitionAssignmentStrategy strategy = cacheService.getPartitionAssignmentStrategy();

		// If partition assignment strategy is null, then can only be highest level of machine safe
		String sStatus = (strategy == null) ? MACHINE_SAFE : SITE_SAFE;

		int count = cacheService.getPartitionCount();

		/*
		 * Start with the highest level SITE_SAFE and work backwards towards ENDANGERED. Note: We are not accounting for any partitions
		 * being in-flight. site-name and rack-name may not be specified, but machine will be either auto-generated or set.
		 */
		for (int i = 0; i < count; i++)
		{
			Member memberOwner = cacheService.getPartitionOwner(i);

			// Assuming backup-count=1
			Member memberBackup = cacheService.getBackupOwner(i, 1);

			// No backups created or backup on same node, so must be endangered
			/*if ((memberBackup == null || memberOwner == null) || (memberBackup != null && memberOwner != null && (memberOwner.getId() == memberBackup.getId())))
			{
				sStatus = ENDANGERED;
				break;
			}*/
			if ((cacheService.getBackupOwner(i, 1) == null || cacheService.getPartitionOwner(i) == null) || (memberOwner.getId() == memberBackup.getId()) )
			{
				sStatus = ENDANGERED;
				break;
			} 

			if (SITE_SAFE.equals(sStatus))
			{
				// if no site-name specified or the site-name specified = the backup site, then not site safe
				if ((memberOwner.getSiteName() == null || memberBackup.getSiteName() == null)
						|| memberOwner.getSiteName().equals(memberBackup.getSiteName()))
				{
					sStatus = RACK_SAFE;
				}
			}

			if (RACK_SAFE.equals(sStatus))
			{
				// if no rack-name specified or the rack-name specified = the backup rack, then not rack safe
				if ((memberOwner.getRackName() == null || memberBackup.getRackName() == null)
						|| memberOwner.getRackName().equals(memberBackup.getRackName()))
				{
					sStatus = MACHINE_SAFE;
				}
			}

			// if on the same machine then must be at most node safe
			if (memberOwner.getMachineId() == memberBackup.getMachineId())
			{
				sStatus = NODE_SAFE;
			}

		}

		return sStatus;
	}

	/**
	 * Returns a Map of the member PID's and the primary partitions counts they own. A pseudo member id of -1 indicates un-owned partitions.
	 * We are using PID and not memberId as the current implementation of the incubator does not expose this.
	 * 
	 * @param cacheService
	 *            {@DistributedCacheService} we want to analyze
	 * 
	 * @return a map of <Long, Integer> representing the PID and primary partitions owned
	 */
	public static Map<Long, Integer> getPartitionsOwned(DistributedCacheService cacheService)
	{
		Map<Long, Integer> mapPartitions = new HashMap<Long, Integer>();
		int currentCount = 0;
		Integer count;
		long memberPID;

		int paritions = cacheService.getPartitionCount();

		for (int i = 0; i < paritions; i++)
		{
			Member owner = cacheService.getPartitionOwner(i);
			memberPID = (owner == null) ? Long.valueOf(-1) : Long.parseLong(owner.getProcessName());

			// if (owner != null)
			// System.out.println("Partition number " + i + " owner is " + owner.getProcessName());

			count = mapPartitions.get(memberPID);

			if (count == null)
			{
				// no entry so create one and initialize to zero
				mapPartitions.put(Long.valueOf(memberPID), Integer.valueOf(0));
				currentCount = 0;
			}
			else {
				currentCount = count.intValue();
			}

			currentCount++;

			mapPartitions.put(Long.valueOf(memberPID), Integer.valueOf(currentCount));

		}

		return mapPartitions;
	}

	// ---- static -----------------------------------------------------------

}
