# Benchmark TiKV and Rocksdb

## 1. Benchmark TiKV and Rocksdb on a single machine
I have deployed two pd-servers and each pd-server manages one tikv-server.

(1) The following tests are all based on 100 operations and we record the average time for each operation.
### Insert raw node/edge property operation 
|    TiKV     |    Rocksdb    |
|------------ |---------------|
|    26~34ms  |    55~105ms   |

### Extend one raw node/edge property operation
|  TiKV        |    Rocksdb      |
|--------------|-----------------|
|  0.24~0.34ms |    0.4~1.3ms    |

### Get node/edge property(all) operation
Note that this is not a fair comparison because the Rocksdb's `get` operation in `rust_graph_lib` is simply fetch k-v from memory(it reads all k-v pairs into memory when connecting to it, it is more like the `tikv`'s `batch_get` operation) while `TiKV` needs to connect to pd-server and reads data from disk into memory to return it. With the connection time and reading all kv pairs into memory time counted in, the single get operation for rocksdb is actually around `105ms`.

|  TiKV  |    Rocksdb   |
|--------|--------------|
|  3~4ms |  0.03~0.06ms |

(2) The following `batch_get` operation test is based on batchly get 1000 keys and we record the average time.
### Batch get node/edge property(all) operation
 |   TiKV   | 
 |----------|
 |  0.008ms | 

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

### Batch get node/edge property(all) operation
|       TiKV        |
|-------------------|
|  0.008ms ~ 0.01ms |

(Batch get 1000 node/edge properties, and it takes 0.008s ~ 0.01s in total)

## 1. `Batch` operations performance comparing between TiKV on a cluster and RocksDB
I have deployed two pd-servers and each pd-server manages four tikv-servers(totally there are two pd-servers and eight tikv-servers and they are all on different machines).

### Batch put operation on DG10
|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|15229.456ms|867.270ms|
|500|455.471ms|860.938ms|
|1000|453.438ms|880.744ms|

### Batch put operation on DG60
|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|3331.421ms|IO error|
|500|3312.793ms|IO error|
|1000|(pending)|(pending)|