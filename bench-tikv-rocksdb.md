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

## 3. `Batch` operations performance comparing between TiKV on a cluster and RocksDB
I have deployed three pd-servers and each pd-server manages three tikv-servers(totally there are three pd-servers and six tikv-servers and they are all on different machines).
And using one server to running test program.

### Batch put operation on DG10
1. current_thread::Runtime  

|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|610.306s|873.912s|
|500|450.900s|774.325s|
|1000|531.744s|755.248s|
|10000|1739.592s|751.675s|

2. tokio::Runtime(ThreadPool)  

|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|357.481s|776.099s|
|500|427.291s|873.144s|
|1000|528.148s|786.991s|
|10000|2039.108s|784.073s|

### Batch put operation on DG60
|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|2525.971s|13516.203s|
|500|2666.998s|8638.527s|
|1000|2654.622s|8247.292s|
|10000|4054.167s|8104.632s|

### Batch get node/edge property(all) operation
|Batch Size|TiKV|RocksDB|
|---|---|---|
|100|0.078s ~ 0.079s|0.387s ~ 0.425s|
|500|0.382s ~ 0.387s|1.848s ~ 1.938s|
|1000|0.468s ~ 0.476s|3.869s ~ 3.871s|
(RocksDB using `while` iteration to simulate `batch_get`)