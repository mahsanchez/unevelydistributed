package com.mahsanchez.coherence.datacaching.management;

import com.mahsanchez.coherence.datacaching.management.util.PartitionHelper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.Service;
import com.tangosol.net.DistributedCacheService;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;


/*
 * Return the status of each Coherence Distributed Service in the Cluster.
 * */
public class CoherenceClusterStatus {
	 final static String DISTRIBUTED_CACHE = "DistributedCache";
     final static String CLUSTER_SERVICE = "Cluster";
     final static String MANAGEMENT_SERVICE = "Management";
     final static long DEFAULT_TIMEOUT = 25000L; // 25s
     private final ExecutorService executor;

     public CoherenceClusterStatus() {
         executor = Executors.newSingleThreadExecutor();
     }
     /**
      * Returns a Map reflecting the statusHA of every DistributedCache service in the cluster
      * @return the map that contains tuples (service_name, statusHA)
      */
     public Map<String, String> getServicesStatusHA() {
         Map<String, String> result = new HashMap<>();
         Cluster cluster = CacheFactory.ensureCluster();
         Enumeration services = cluster.getServiceNames();
         /*
          * Scan all Distributed Cache Services Services under this CacheFactory
          */
         if (services == null) {
             throw new RuntimeException("There is any Coherence Service available in this Cluster");
         }
         while ( services.hasMoreElements() ) {
             String serviceName = (String) services.nextElement();
             if (!(serviceName.equals(CLUSTER_SERVICE)) && !(serviceName.equals(MANAGEMENT_SERVICE)) ) {
                 try {
                     Service cacheService = CacheFactory.getService(serviceName);
                     if ( cacheService.getInfo().getServiceType().equals(DISTRIBUTED_CACHE) ) {
                         DistributedCacheService  distributedCacheService = (DistributedCacheService ) cacheService;
                         String statusHA = PartitionHelper.getStatusHA( distributedCacheService );
                         result.put(serviceName, statusHA);
                     }
                 }
                 catch (Exception exception) {
                     throw new RuntimeException(exception);
                 }
             }
         }
         return result;
     }

    /**
     * Returns a Map reflecting the statusHA of every DistributedCache service in the cluster
     * this method TimeOut in case the method getServicesStatusHA take too long
     * @param timeOut in MILLISECONDS
     * @return the map that contains tuples (service_name, statusHA)
     */
    public Map<String, String> getServicesStatusHA(long timeOut) {
        Map<String, String> result = Collections.EMPTY_MAP;
        try {
            Future< Map<String, String> > future = executor.submit(
                    new Callable< Map<String, String> > () {
                        public Map<String, String> call() {
                            return getServicesStatusHA();
                        }
                    }
            );
            result = future.get(timeOut, TimeUnit.MILLISECONDS);
        }
        catch(TimeoutException exception) {
            log("TimeOut retrieving service statusHA");
        }
        catch(Exception exception) {
            throw new RuntimeException(exception);
        }
        return result;
    }
	 
     static public void main(String args[]) {
         long timeOut = DEFAULT_TIMEOUT;
         try {
             if (args.length > 0) {
                 timeOut = Long.parseLong( args[0] );
             }
         }
         catch(NumberFormatException exception) {
             log( String.format("Using default timeOut value %d", DEFAULT_TIMEOUT) );
         }
         // Retrieve Distributed Services StatusHA
         final CoherenceClusterStatus clusterStatusClient = new CoherenceClusterStatus();
         try {
             Map<String, String> servicesStatusHAMap = clusterStatusClient.getServicesStatusHA(timeOut);
             for (Map.Entry<String, String> entry : servicesStatusHAMap.entrySet()) {
                 log( String.format(" ServiceName: %s, Status: %s", entry.getKey(), entry.getValue()) );
             }
         }
         catch(Exception exception) {
             log( String.format("Error retrieving the Coherence Distributed Cache Services active in the Cluster Cause: %s", exception.getStackTrace() ) );
         }
     }

     
     /**
      */
     static public void log(String message) {
  	   System.out.println( new java.util.Date() + message );
     }
     
}
