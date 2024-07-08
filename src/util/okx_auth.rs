use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::encode;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
pub struct OkxAuth {
    api_key: String,
    api_secret_key: String,
    passphrase: String,
}

#[derive(Serialize)]
pub struct LoginArgs {
    apiKey: String,
    passphrase: String,
    timestamp: String,
    sign: String,
}

#[derive(Serialize)]
pub struct LoginParam {
    op: String,
    args: Vec<LoginArgs>,
}

impl OkxAuth {
    pub fn new(api_key: &str, api_secret_key: &str, passphrase: &str) -> Self {
        OkxAuth {
            api_key: api_key.to_string(),
            api_secret_key: api_secret_key.to_string(),
            passphrase: passphrase.to_string(),
        }
    }

    pub fn get_local_timestamp() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    pub fn okx_login_params(&self) -> LoginParam {
        let ts = OkxAuth::get_local_timestamp().to_string();
        let message = format!("{}GET/users/self/verify", ts);

        let mut mac = HmacSha256::new_from_slice(self.api_secret_key.as_bytes()).expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();
        let code_bytes = result.into_bytes();
        let sign = encode(&code_bytes);

        LoginParam {
            op: "login".to_string(),
            args: vec![LoginArgs {
                apiKey: self.api_key.clone(),
                passphrase: self.passphrase.clone(),
                timestamp: ts,
                sign,
            }],
        }
    }
}