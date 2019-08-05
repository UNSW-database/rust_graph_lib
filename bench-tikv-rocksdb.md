## Benchmark TiKV and Rocksdb on a single machine

## Single insert raw node/edge property operation 
|    TikV     |     Rocksdb   |
|------------ |---------------|
|   90~100ms  |     55ms      |

## Extend one raw node/edge property operation
|  TikV  |    Rocksdb    |
|--------|---------------|
|  0.9ms |    0.8ms      |

## Get node/edge property(all) operation
|  TikV  |    Rocksdb    |
|--------|---------------|
|  5~6ms |    4~5ms      |

