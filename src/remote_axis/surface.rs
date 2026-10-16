//! Pure publication policy for the worker's remote plugin axis.
//! The host fixes participant identities and hooks. Remote input can only select them.
use std::{collections::BTreeMap, sync::Arc};

pub(crate) const CONTRACT: &str = "worker.metadata.v1";

pub(crate) struct Grant {
    pub axis: String,
    pub registration: String,
    methods: BTreeMap<String, String>,
    codec: Option<String>,
}
impl Grant {
    pub fn new(axis: String, registration: String, hooks: &[String]) -> Self {
        let methods = hooks
            .iter()
            .map(|hook| (hook.clone(), format!("{CONTRACT}.{registration}.{hook}")))
            .collect();
        Self {
            axis,
            registration,
            methods,
            codec: None,
        }
    }
    pub fn with_codec(mut self, codec: Option<String>) -> Self {
        self.codec = codec;
        self
    }
    pub fn message_method(&self, method: &str) -> Result<String, String> {
        let fields: Vec<_> = method.split('/').collect();
        if fields.len() != 4
            || self.codec.as_deref() != Some(fields[0])
            || !crate::proxy::message_codec::valid_id(fields[1])
            || fields[1].len() > 64
            || fields[2..]
                .iter()
                .any(|id| id.len() != 16 || !id.bytes().all(|b| b.is_ascii_hexdigit()))
            || method.len() > 256
        {
            return Err("message method is outside the issued plugin grant".into());
        }
        Ok(format!("worker.message.v1.{}", self.registration))
    }
    pub fn method(&self, hook: &str) -> Result<&str, String> {
        self.methods
            .get(hook)
            .map(String::as_str)
            .ok_or_else(|| "hook is outside the issued plugin grant".into())
    }
}

#[derive(Default)]
pub(crate) struct Surface {
    grants: BTreeMap<(String, String), Arc<Grant>>,
}
impl Surface {
    pub fn new(selected: Vec<Grant>) -> Result<Self, String> {
        let mut grants = BTreeMap::new();
        for grant in selected {
            let key = (grant.axis.clone(), grant.registration.clone());
            if grants.insert(key, Arc::new(grant)).is_some() {
                return Err("duplicate remote plugin grant".into());
            }
        }
        Ok(Self { grants })
    }
    pub fn select(&self, axis: &str, registration: &str) -> Result<Arc<Grant>, String> {
        self.grants
            .get(&(axis.to_owned(), registration.to_owned()))
            .cloned()
            .ok_or_else(|| "plugin registration is outside the host publication".into())
    }
    pub fn grants(&self) -> impl Iterator<Item = &Arc<Grant>> {
        self.grants.values()
    }
}
