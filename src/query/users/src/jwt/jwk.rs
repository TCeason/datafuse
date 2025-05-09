// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use base64::engine::general_purpose;
use base64::prelude::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jwt_simple::prelude::ES256PublicKey;
use jwt_simple::prelude::RS256PublicKey;
use log::info;
use log::warn;
use p256::EncodedPoint;
use p256::FieldBytes;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

use super::PubKey;

const JWKS_REFRESH_TIMEOUT: u64 = 10;
const JWKS_REFRESH_INTERVAL: u64 = 600;

#[derive(Debug, Serialize, Deserialize)]
pub struct JwkKey {
    pub kid: String,
    pub kty: String,
    pub alg: Option<String>,

    /// (Modulus) Parameter for kty `RSA`.
    #[serde(default)]
    pub n: String,
    /// (Exponent) Parameter for kty `RSA`.
    #[serde(default)]
    pub e: String,

    /// (X Coordinate) Parameter for kty `EC`
    #[serde(default)]
    pub x: String,
    /// (Y Coordinate) Parameter for kty `EC`
    #[serde(default)]
    pub y: String,
}

fn decode(v: &str) -> Result<Vec<u8>> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(v.as_bytes())
        .map_err(|e| ErrorCode::InvalidConfig(e.to_string()))
}

impl JwkKey {
    fn get_public_key(&self) -> Result<PubKey> {
        match self.kty.as_str() {
            // Todo(youngsofun): the "alg" field is optional, maybe we need a config for it
            "RSA" => {
                let k = RS256PublicKey::from_components(&decode(&self.n)?, &decode(&self.e)?)?;
                Ok(PubKey::RSA256(Box::new(k)))
            }
            "EC" => {
                // borrowed from https://github.com/RustCrypto/traits/blob/master/elliptic-curve/src/jwk.rs#L68
                let xs = decode(&self.x)?;
                let x = FieldBytes::from_slice(&xs);
                let ys = decode(&self.y)?;
                let y = FieldBytes::from_slice(&ys);
                let ep = EncodedPoint::from_affine_coordinates(x, y, false);

                let k = ES256PublicKey::from_bytes(ep.as_bytes())?;
                Ok(PubKey::ES256(k))
            }
            _ => Err(ErrorCode::InvalidConfig(format!(
                " current not support jwk with typ={:?}",
                self.kty
            ))),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct JwkKeys {
    pub keys: Vec<JwkKey>,
}

/// [`JwkKeyStore`] is a store for JWKS keys, it will cache the keys for a while and refresh the
/// keys periodically. When the keys are refreshed, the older keys will still be kept for a while.
///
/// When the keys rotated in the client side first, the server will respond a 401 Authorization Failure
/// error, as the key is not found in the cache. We'll try to refresh the keys and try again.
pub struct JwkKeyStore {
    url: String,
    recent_cached_maps: Arc<RwLock<VecDeque<HashMap<String, PubKey>>>>,
    last_refreshed_time: RwLock<Option<Instant>>,
    last_retry_time: RwLock<Option<Instant>>,
    max_recent_cached_maps: usize,
    refresh_interval: Duration,
    refresh_timeout: Duration,
    retry_interval: Duration,
    load_keys_func: Option<Arc<dyn Fn() -> HashMap<String, PubKey> + Send + Sync>>,
}

impl JwkKeyStore {
    pub fn new(url: String) -> Self {
        Self {
            url,
            recent_cached_maps: Arc::new(RwLock::new(VecDeque::new())),
            max_recent_cached_maps: 2,
            refresh_interval: Duration::from_secs(JWKS_REFRESH_INTERVAL),
            refresh_timeout: Duration::from_secs(JWKS_REFRESH_TIMEOUT),
            retry_interval: Duration::from_secs(2),
            last_refreshed_time: RwLock::new(None),
            last_retry_time: RwLock::new(None),
            load_keys_func: None,
        }
    }

    // only for test to mock the keys
    pub fn with_load_keys_func(
        mut self,
        func: Arc<dyn Fn() -> HashMap<String, PubKey> + Send + Sync>,
    ) -> Self {
        self.load_keys_func = Some(func);
        self
    }

    pub fn with_refresh_interval(mut self, interval: u64) -> Self {
        self.refresh_interval = Duration::from_secs(interval);
        self
    }

    pub fn with_refresh_timeout(mut self, timeout: u64) -> Self {
        self.refresh_timeout = Duration::from_secs(timeout);
        self
    }

    pub fn with_max_recent_cached_maps(mut self, max: usize) -> Self {
        self.max_recent_cached_maps = max;
        self
    }

    pub fn with_retry_interval(mut self, interval: u64) -> Self {
        self.retry_interval = Duration::from_secs(interval);
        self
    }

    pub fn url(&self) -> String {
        self.url.clone()
    }
}

impl JwkKeyStore {
    #[async_backtrace::framed]
    async fn load_keys(&self) -> Result<HashMap<String, PubKey>> {
        if let Some(load_keys_func) = &self.load_keys_func {
            return Ok(load_keys_func());
        }

        let client = reqwest::Client::builder()
            .timeout(self.refresh_timeout)
            .build()
            .map_err(|e| {
                ErrorCode::InvalidConfig(format!("Failed to create jwks client: {}", e))
            })?;
        let response = client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| ErrorCode::Internal(format!("Could not download JWKS: {}", e)))?;
        let jwk_keys: JwkKeys = response
            .json()
            .await
            .map_err(|e| ErrorCode::InvalidConfig(format!("Failed to parse JWKS: {}", e)))?;
        let mut new_keys: HashMap<String, PubKey> = HashMap::new();
        for k in &jwk_keys.keys {
            new_keys.insert(k.kid.to_string(), k.get_public_key()?);
        }
        Ok(new_keys)
    }

    #[async_backtrace::framed]
    async fn maybe_refresh_cached_keys(&self, force: bool) -> Result<()> {
        let need_reload = force
            || match *self.last_refreshed_time.read() {
                None => true,
                Some(last_refreshed_at) => last_refreshed_at.elapsed() > self.refresh_interval,
            };

        let old_keys = self
            .recent_cached_maps
            .read()
            .iter()
            .last()
            .cloned()
            .unwrap_or(HashMap::new());
        if !need_reload {
            return Ok(());
        }

        // if got network issues on loading JWKS, fallback to the cached keys if available
        let new_keys = match self.load_keys().await {
            Ok(new_keys) => new_keys,
            Err(err) => {
                warn!("Failed to load JWKS: {}", err);
                if !old_keys.is_empty() {
                    return Ok(());
                }
                return Err(err.add_message("failed to load JWKS keys, and no available fallback"));
            }
        };

        // if the new keys are empty, skip save it to the cache
        if new_keys.is_empty() {
            warn!("got empty JWKS keys, skip");
            return Ok(());
        }

        // only update the cache when the keys are changed
        if new_keys.keys().eq(old_keys.keys()) {
            return Ok(());
        }
        info!("JWKS keys changed.");

        // append the new keys to the end of recent_cached_maps
        {
            let mut recent_cached_maps = self.recent_cached_maps.write();
            recent_cached_maps.push_back(new_keys);
            if recent_cached_maps.len() > self.max_recent_cached_maps {
                recent_cached_maps.pop_front();
            }
        }
        self.last_refreshed_time.write().replace(Instant::now());
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn get_key(&self, key_id: &Option<String>) -> Result<Option<PubKey>> {
        self.maybe_refresh_cached_keys(false).await?;

        // if the key_id is not set, and there is only one key in the store, return it
        let key_id = match &key_id {
            Some(key_id) => key_id.clone(),
            None => {
                let cached_maps = self.recent_cached_maps.read();
                let first_key = cached_maps
                    .iter()
                    .last()
                    .and_then(|keys| keys.iter().next());
                if let Some((_, pub_key)) = first_key {
                    return Ok(Some(pub_key.clone()));
                } else {
                    return Err(ErrorCode::AuthenticateFailure(
                        "must specify key_id for jwt when multi keys exists ",
                    ));
                }
            }
        };

        // if the key is not found, try to refresh the keys and try again. this refresh only
        // happens once within retry interval (default 2s).
        for _ in 0..2 {
            for keys_map in self.recent_cached_maps.read().iter().rev() {
                if let Some(key) = keys_map.get(&key_id) {
                    return Ok(Some(key.clone()));
                }
            }
            let need_retry = match *self.last_retry_time.read() {
                None => true,
                Some(last_retry_time) => last_retry_time.elapsed() > self.retry_interval,
            };
            if need_retry {
                warn!(
                    "key id {} not found in jwk store, try to peek the latest keys",
                    key_id
                );
                self.maybe_refresh_cached_keys(true).await?;
                *self.last_retry_time.write() = Some(Instant::now());
            }
        }

        Ok(None)
    }
}
