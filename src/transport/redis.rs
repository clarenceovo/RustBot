use redis::{Client, Commands, RedisResult};

pub struct RedisClient {
    client: Client,
}

impl RedisClient {
    pub fn new(connection_url: &str) -> RedisResult<Self> {
        let client = Client::open(connection_url)?;
        Ok(RedisClient { client })
    }

    pub async fn set(&self, key: &str, value: &str) -> RedisResult<()> {
        let mut connection = self.client.get_connection()?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut connection)
    }

    pub async fn get(&self, key: &str) -> RedisResult<Option<String>> {
        let mut connection = self.client.get_connection()?;
        redis::cmd("GET")
            .arg(key)
            .query(&mut connection)
    }

    pub async fn delete(&self, key: &str) -> RedisResult<()> {
        let mut connection = self.client.get_connection()?;
        redis::cmd("DEL")
            .arg(key)
            .query(&mut connection)
    }
}