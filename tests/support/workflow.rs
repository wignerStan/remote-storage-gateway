//! Scenario evidence uses the shared infra classifier and bounded trace writer.
use diagnostic_output::CredentialPolicy;
use serde_json::Value;
use test_support::workflow::WorkflowTrace;

pub struct Evidence(Option<WorkflowTrace<Vec<u8>>>);

impl Evidence {
    pub fn new() -> Self {
        Self(Some(WorkflowTrace::new(
            Vec::new(),
            CredentialPolicy::default(),
            8192,
        )))
    }

    pub fn record(&mut self, step: &str, action: &str, reason: &str, facts: Value) {
        self.0
            .as_mut()
            .unwrap()
            .decision(step, action, reason, facts)
            .unwrap();
    }
}

impl Drop for Evidence {
    fn drop(&mut self) {
        // Print once per scenario, including the evidence before a failed assertion.
        if let Some(trace) = self.0.take() {
            eprintln!("{}", String::from_utf8_lossy(&trace.into_inner()));
        }
    }
}
