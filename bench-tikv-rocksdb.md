# Benchmark TiKV and Rocksdb

## 1. Benchmark TiKV and Rocksdb on a single machine
I have deployed two pd-servers and each pd-server manages one tikv-server.

### Insert raw node/edge property operation 
|    TikV     |     Rocksdb   |
|------------ |---------------|
|     34ms    |     55ms      |

### Extend one raw node/edge property operation
|  TikV  |    Rocksdb    |
|--------|---------------|
|  0.25ms |    0.7ms     |

### Get node/edge property(all) operation
|  TikV  |    Rocksdb    |
|--------|---------------|
|  5~6ms |    0.03ms     |

## 2. Benchmark TiKV on a cluster
I have deployed two pd-servers and each pd-server manages four tikv-servers(totally there are two pd-servers and eight tikv-servers and they are all on different machines).

### Insert raw node/edge property operation 
|    TikV     |   
|------------ |
|     90ms    | 

### Extend one raw node/edge property operation
|  TikV  |
|--------|
|  0.9ms | 

### Get node/edge property(all) operation
|  TikV  |
|--------|
|  4~5ms |

