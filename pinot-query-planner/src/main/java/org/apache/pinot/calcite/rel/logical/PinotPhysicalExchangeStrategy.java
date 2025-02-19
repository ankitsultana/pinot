package org.apache.pinot.calcite.rel.logical;

public enum PinotPhysicalExchangeStrategy {
  SINGLETON,
  BROADCAST,
  /** worker-id X sends to worker-id X. */
  IDENTITY,
  /** splits stream of worker-id X based on partition key, and sends to workers X, X + N, X + 2*N, ... */
  LOCAL_HASH_PARTITIONING,
  /** randomly splits stream of worker-id X, and sends to workers X, X + N, X + 2*N, ... */
  LOCAL_PARALLELISM,
  /** full fan-out shuffle; worker-id X sends to all workers on receiver. */
  HASH;
}
