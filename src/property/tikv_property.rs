use tikv_client::Config;
use crate::property::PropertyError;
use tikv_client::raw::Client;

pub struct TikvProperty {
    config: Config
}

impl TikvProperty {
    pub fn new(config: Config) -> Result<Self, PropertyError> {
        Ok(TikvProperty {
            config
        })
    }
}

#[cfg(test)]
mod test {
    extern crate tikv_client;

    use tikv_client::{*};
    use tikv_client::raw::Client;

    #[test]
    fn test_tikv_put_get() {
        futures::executor::block_on(
            async {
                let connect = Client::connect(Config::new(vec!["192.168.2.2"]));
                let client = connect.await.unwrap();
                const KEY: &str = "Tikv";
                const VALUE: &str = "Rust";
                client.put(KEY.to_owned(), VALUE.to_owned()).await.unwrap();
                println!("Put key {:?}, value {:?}.", KEY, VALUE);

                let value: Option<Value> = client.get(KEY.to_owned()).await?;
                assert_eq!(value, Some(Value::from(VALUE.to_owned())));
                println!("Get key `{}` returned value {:?}.", KEY, value);
            }
        );
    }
}