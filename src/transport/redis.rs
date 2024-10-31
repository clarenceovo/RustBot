use redis::aio::Connection;
use redis::{AsyncCommands, Client, RedisResult, Cmd, Value};
use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct RedisClient {
    client: Client,
    connection: Mutex<Connection>, // Ensures safe concurrent access
}

impl RedisClient {
    /// Initializes a new RedisClient with an asynchronous connection.
    pub async fn new(host: &str, port: u16, password: Option<&str>) -> RedisResult<Self> {
        let connection_url = match password {
            Some(pass) => format!("redis://:{}@{}:{}", pass, host, port),
            None => format!("redis://{}:{}", host, port),
        };

        let client = Client::open(connection_url)?;
        let connection = client.get_async_connection().await?;
        Ok(RedisClient {
            client,
            connection: Mutex::new(connection),
        })
    }

    /// Sets a key-value pair asynchronously.
    pub async fn set(&self, key: &str, value: &str) -> RedisResult<()> {
        let mut conn = self.connection.lock().await;
        conn.set(key, value).await
    }

    /// Retrieves the value for a given key asynchronously.
    pub async fn get(&self, key: &str) -> RedisResult<Option<String>> {
        let mut conn = self.connection.lock().await;
        conn.get(key).await
    }

    /// Sets a field in a hash asynchronously.
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> RedisResult<()> {
        let mut conn = self.connection.lock().await;
        conn.hset(key, field, value).await
    }

    pub async fn hset_multiple(&self, key: &str, map: HashMap<&str, &str>) -> RedisResult<()> {
        // Convert the HashMap into a Vec of tuples
        let map_vec: Vec<(&str, &str)> = map.into_iter().collect();

        // Acquire the lock and set multiple fields in the hash
        let mut conn = self.connection.lock().await;
        conn.hset_multiple(key, &map_vec).await
    }

    /// Deletes a key asynchronously.
    pub async fn delete(&self, key: &str) -> RedisResult<()> {
        let mut conn = self.connection.lock().await;
        conn.del(key).await
    }

    /// Executes multiple SET commands in a pipeline asynchronously.
    pub async fn pipeline_set(&self, key_values: Vec<(&str, &str)>) -> RedisResult<()> {
        let mut conn = self.connection.lock().await;
        let mut pipe = redis::pipe();

        for (key, value) in key_values {
            pipe.cmd("SET").arg(key).arg(value);
        }

        pipe.query_async(&mut *conn).await?;
        Ok(())
    }

    /// Executes a mixed set of commands in a pipeline asynchronously and returns their results.
    pub async fn pipeline_mixed(&self, commands: Vec<Cmd>) -> RedisResult<Vec<Value>> {
        let mut conn = self.connection.lock().await;
        let mut pipe = redis::pipe();

        for cmd in commands {
            pipe.add_command(cmd);
        }

        pipe.query_async(&mut *conn).await
    }
}