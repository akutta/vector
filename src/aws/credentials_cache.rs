//! Utility for caching AWS credentials when not using a standard AWS Client
use std::time::SystemTime;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::provider::error::CredentialsError;

/// A utility for caching AWS credentials.
#[derive(Clone, Debug)]
pub struct CredentialsCache {
    credentials: Option<Credentials>,
    provider: SharedCredentialsProvider,
    next_refresh: SystemTime
}

impl CredentialsCache {
    /// Creates a new `CredentialsCache` instance.
    pub fn new(provider: SharedCredentialsProvider) -> Self {
        Self { credentials: None, provider, next_refresh: SystemTime::now() }
    }

    /// Provides credentials, refreshing them if necessary.
    pub async fn provide_credentials(&mut self) -> Result<Credentials, CredentialsError> {
        info!("credentials_cache::provide_credentials credentials: {}", self.credentials.is_some());
        if self.next_refresh < SystemTime::now() {
            let new_credentials = self.provider.provide_credentials().await?;
            let next_refresh = new_credentials.expiry().unwrap_or(SystemTime::now()).duration_since(SystemTime::now())
                .unwrap_or(std::time::Duration::from_secs(0))
                .as_secs();
            self.next_refresh = SystemTime::now() + std::time::Duration::from_secs(next_refresh);
            self.credentials = Some(new_credentials);
        }

        info!("credentials_cache::provide_credentials returning credentials: {}", self.credentials.clone().unwrap().access_key_id());

        Ok(self.credentials.clone().expect("unable to retrieve cached credentials"))
    }
}

