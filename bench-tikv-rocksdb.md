# Benchmark TiKV and Rocksdb

## 1. Benchmark TiKV and Rocksdb on a single machine
I have deployed two pd-servers and each pd-server manages one tikv-server.

### Insert raw node/edge property operation 
|    TiKV     |     Rocksdb   |
|------------ |---------------|
|     34ms    |     55~76ms   |

### Extend one raw node/edge property operation
|  TiKV   |    Rocksdb      |
|---------|-----------------|
|  0.25ms |    0.4~0.7ms    |

### Get node/edge property(all) operation
Note that this is not a fair comparison because the Rocksdb's `get` operation in `rust_graph_lib` is simply fetch k-v from memory(it reads all k-v pairs into memory when connecting to it) while `TiKV` needs to connect to pd-server and reads data from disk into memory to return it.

|  TiKV  |    Rocksdb    |
|--------|---------------|
|  5~6ms |    0.03ms     |

## 2. Benchmark TiKV on a cluster
I have deployed two pd-servers and each pd-server manages four tikv-servers(totally there are two pd-servers and eight tikv-servers and they are all on different machines).

### Insert raw node/edge property operation 
|    TiKV     |   
|------------ |
|     90ms    | 

### Extend one raw node/edge property operation
|    TiKV    |
|------------|
|  0.6~0.9ms | 

### Get node/edge property(all) operation
|  TiKV  |
|--------|
|  4~5ms |

