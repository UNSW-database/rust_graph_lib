## 1. Deploy TiKV
You can refer to [deploy-tikv](https://github.com/tikv/tikv/blob/master/docs/how-to/deploy/using-binary.md) to easily deploy a TikV cluster on a single machine or on a cluster.

### Example of deploying two Tikv clusters in ECNU cluster
We can deploy two Tikv clusters on ecnu00(to store node properties) and ecnu01(to store edge properties) respectively: 
```shell script
## deploy one Tikv cluster on ecnu00 used as the node properties storage engine
ssh ecnu00
cd Shares/tidb-latest-linux-amd64
./bin/pd-server --name=pd1 --data-dir=pd1 --client-urls="http://192.168.2.2:2379"  --peer-urls="http://192.168.2.2:2380" --initial-cluster="pd1=http://192.168.2.2:2380" --log-file=pd1.log &

./bin/tikv-server --pd-endpoints="192.168.2.2:2379" --addr="192.168.2.2:20160" --data-dir=tikv-ecnu00-1 --log-file=tikv-ecnu00-1.log &

./bin/tikv-server --pd-endpoints="192.168.2.2:2379" --addr="192.168.2.2:20161" --data-dir=tikv-ecnu00-2 --log-file=tikv-ecnu00-2.log &

./bin/tikv-server --pd-endpoints="192.168.2.2:2379" --addr="192.168.2.2:20162" --data-dir=tikv-ecnu00-3 --log-file=tikv-ecnu00-3.log &

## Check tikv-servers' status, if you start them, you should see all the tikv instances with status `Up` 
./bin/pd-ctl store -d -u http://ecnu00:2379

## deploy one Tikv clusters on ecnu01 used as the edge properties storage engine
ssh ecnu01
cd Shares/tidb-latest-linux-amd64
./bin/pd-server --name=pd2 --data-dir=pd2 --client-urls="http://192.168.2.3:2379"  --peer-urls="http://192.168.2.3:2380" --initial-cluster="pd2=http://192.168.2.3:2380" --log-file=pd2.log &

./bin/tikv-server --pd-endpoints="192.168.2.3:2379" --addr="192.168.2.3:20160" --data-dir=tikv-ecnu01-1 --log-file=tikv-ecnu01-1.log &

./bin/tikv-server --pd-endpoints="192.168.2.3:2379" --addr="192.168.2.3:20161" --data-dir=tikv-ecnu01-2 --log-file=tikv-ecnu01-2.log &

./bin/tikv-server --pd-endpoints="192.168.2.3:2379" --addr="192.168.2.3:20162" --data-dir=tikv-ecnu01-3 --log-file=tikv-ecnu01-3.log &

## Check tikv-servers' status, if you start them, you should see all the tikv instances with status `Up` 
./bin/pd-ctl store -d -u http://ecnu01:2379
```
What's more, you can also deploy the tikv-servers on different machines, the upper example deploys tikv-servers on a single machine.

## 2. Connect to TiKV Server
You can add the official [tikv-client](https://github.com/tikv/client-rust) dependency to your `Cargo.toml` file and use it to interact with TiKV:
```toml
[dependencies]
# ...Your other dependencies...
tikv-client = { git = "https://github.com/tikv/client-rust.git" }
```
Note that since many interfaces in `tikv-client` are `async`, you have to use rust edition 2018 to use the keyword `async` and `await`. You should first use command `cargo fix --edition` then add `edition=2018` to your `Cargo.toml`:
```toml
[package]
# ...Your package settings...
edition="2018"
```
Then, you can connect to `TiKV` now. In fact, we connect to pd-server(s) not tikv-server(s), where pd-server is to manage the tikv-server(s) including the meta-data management and load balance, and tikv-server is the K-V storage engine.

Note that this project should be built in Linux or WSL, and you have to type command `rustup default nightly` to use nightly.

Here is an example to connect to `TiKV`:

```rust
#![feature(async_await)]
use tikv_client::{raw::Client, Config, Key, KvPair, Result, Value};

const KEY: &str = "TiKV";
const VALUE: &str = "Rust";

async fn main() -> Result<()> {
    // Create a configuration to use for the example.
    let config = Config::new(vec!["192.168.2.2:2379"]);

    // When we first create a client we receive a `Connect` structure which must be resolved before
    // the client is actually connected and usable.
    let unconnnected_client = Client::connect(config);
    let client = unconnnected_client.await?;

    // Requests are created from the connected client. These calls return structures which
    // implement `Future`. This means the `Future` must be resolved before the action ever takes
    // place.
    //
    // Here we set the key `TiKV` to have the value `Rust` associated with it.
    client.put(KEY.to_owned(), VALUE.to_owned()).await.unwrap(); // Returns a `tikv_client::Error` on failure.
    println!("Put key {:?}, value {:?}.", KEY, VALUE);

    // Unlike a standard Rust HashMap all calls take owned values. This is because under the hood
    // protobufs must take ownership of the data. If we only took a borrow we'd need to internally
    // clone it. This is against Rust API guidelines, so you must manage this yourself.
    //
    // Above, you saw we can use a `&'static str`, this is primarily for making examples short.
    // This type is practical to use for real things, and usage forces an internal copy.
    //
    // It is best to pass a `Vec<u8>` in terms of explictness and speed. `String`s and a few other
    // types are supported as well, but it all ends up as `Vec<u8>` in the end.
    let value: Option<Value> = client.get(KEY.to_owned()).await?;
    assert_eq!(value, Some(Value::from(VALUE.to_owned())));
    println!("Get key `{}` returned value {:?}.", KEY, value);
}
```

Here is another example using our `rust_graph_lib` to store property graph in `TikV`. The properties are in json format.
```rust
#![feature(async_await)]
extern crate rust_graph;
extern crate serde_json;
extern crate tikv_client;

use rust_graph::property::tikv_property::*;
use rust_graph::property::PropertyGraph;
use serde_json::{json, to_vec};
use tikv_client::Config;

/// The pd-server that is responsible to store node properties in its managed tikv-servers
const NODE_PD_SERVER_ADDR: &str = "192.168.2.2:2379";

/// The pd-server that is responsible to store edge properties in its managed tikv-servers
const EDGE_PD_SERVER_ADDR: &str = "192.168.2.3:2379";

fn main() {
     let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();
    // insert node property
    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();

    graph.insert_node_raw(0u32, raw_prop).unwrap();
    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);
    
    // insert edge property
    let edge_prop = json!({"length":"15"});
    let raw_edge_prop = to_vec(&edge_prop).unwrap();
    
    graph.insert_edge_raw(0u32, 1u32, raw_edge_prop).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();
    
    assert_eq!(Some(json!({"length":"15"})), edge_property);
}
```

Note that you can config some security settings in `tikv_client::Config`. The `tikv_client::Config` structure is defined as:
```rust
pub struct Config {
    pub(crate) pd_endpoints: Vec<String>,
    pub(crate) ca_path: Option<PathBuf>,
    pub(crate) cert_path: Option<PathBuf>,
    pub(crate) key_path: Option<PathBuf>,
    pub(crate) timeout: Duration,
}
```
where `pd_endpoints` is a vector of pd-servers' addresses. By default, `tikv-client` will use an insecure connection over instead of one protected by Transport Layer Security (TLS). Your deployment may have chosen to rely on security measures such as a private network, or a VPN layer to provid secure transmission. To use a TLS secured connection, use the `with_security` method to set the required parameters. TiKV does not currently offer encrypted storage (or encryption-at-rest).