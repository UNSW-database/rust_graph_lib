use tikv_client::{Config, raw::Client};
use futures::Future;


fn main() {
    let config = Config::new(vec![ // Always use more than one PD endpoint!
                                   "192.168.0.100:2379",
                                   "192.168.0.101:2379",
                                   "192.168.0.102:2379",
    ]).with_security( // Configure TLS if used.
                      "root.ca",
                      "internal.cert",
                      "internal.key",
    );

    let unconnected_client = Client::new(config);
    let client = unconnected_client.wait()?; // Block and resolve the future.

    let client = Client::new(config).wait();
// Data stored in TiKV does not need to be UTF-8.
    let key = "TiKV".to_bytes();
    let value = "Astronaut".to_bytes();

// This creates a future that must be resolved.
    let req = client.put(
        key,  // Vec<u8> impl Into<Key>
        value // Vec<u8> impl Into<Value>
    );
    req.wait()?;

    let req = client.get(key);
    let result = req.wait()?;

// `Value` derefs to `Vec<u8>`.
    assert_eq!(result, Some(value));

    let req = client.delete(key);
    req.wait()?;

    let req = client.get(key).wait()?;
    assert_eq!(result, None);
}





