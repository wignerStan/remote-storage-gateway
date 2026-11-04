//! Facility-owned retained generations. Capacity is reserved before physical dial.
use super::*;
use admission_gate::budget::{Reservation, ResourceBudget};
use std::{
    future::Future,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::Instant as TokioInstant,
};

struct Tasks {
    closing: bool,
    sealed: bool,
    jobs: Vec<JoinHandle<std::result::Result<(), String>>>,
    failures: usize,
    last_failure: Option<String>,
}
/// Pre-dial ownership promise, including while the downstream is still attached.
/// Drop wakes shutdown even when dial fails and no task was started.
pub(super) struct GenerationPermit {
    reservation: Option<Reservation>,
    changed: Arc<Notify>,
}
impl Drop for GenerationPermit {
    fn drop(&mut self) {
        drop(self.reservation.take());
        self.changed.notify_one();
    }
}

/// Owns every retained task and joins it before callback resources close.
/// Tasks receive capabilities only; none holds this owner or the proxy Runtime.
pub(crate) struct RetainedSessions {
    capacity: Arc<ResourceBudget>,
    runtime: tokio::runtime::Handle,
    stop: watch::Sender<Option<TokioInstant>>,
    tasks: Mutex<Tasks>,
    changed: Arc<Notify>,
    grace: Duration,
}
fn reap(tasks: &mut Tasks, cx: &mut Context<'_>) {
    let mut i = 0;
    while i < tasks.jobs.len() {
        match std::pin::Pin::new(&mut tasks.jobs[i]).poll(cx) {
            Poll::Pending => i += 1,
            Poll::Ready(result) => {
                drop(tasks.jobs.swap_remove(i));
                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(error) => Some(error.to_string()),
                };
                if let Some(error) = error {
                    tasks.failures = tasks.failures.saturating_add(1);
                    tasks.last_failure = Some(error);
                }
            }
        }
    }
}
impl RetainedSessions {
    pub(crate) fn new(capacity: usize, grace: Duration) -> Arc<Self> {
        Arc::new(Self {
            capacity: ResourceBudget::new(capacity),
            runtime: tokio::runtime::Handle::current(),
            stop: watch::channel(None).0,
            tasks: Mutex::new(Tasks {
                closing: false,
                sealed: false,
                jobs: Vec::new(),
                failures: 0,
                last_failure: None,
            }),
            changed: Arc::new(Notify::new()),
            grace,
        })
    }
    pub(super) fn reserve(&self) -> Result<GenerationPermit> {
        let tasks = self.tasks.lock().expect("retained owner lock");
        if tasks.closing {
            return Err(refusal(503, "retained session owner is shutting down"));
        }
        let reservation = self
            .capacity
            .try_reserve(1)
            .ok_or_else(|| refusal(503, "physical session capacity exhausted"))?;
        Ok(GenerationPermit {
            reservation: Some(reservation),
            changed: Arc::clone(&self.changed),
        })
    }
    /// Bind newly created I/O registrations to the retained bootstrap reactor.
    /// The future may borrow request controls; it is not spawned or detached.
    pub(super) async fn on_runtime<T>(&self, future: impl Future<Output = T>) -> T {
        tokio::pin!(future);
        std::future::poll_fn(|cx| {
            let _context = self.runtime.enter();
            future.as_mut().poll(cx)
        })
        .await
    }
    pub(super) fn subscribe(&self) -> watch::Receiver<Option<TokioInstant>> {
        self.stop.subscribe()
    }
    pub(super) fn spawn(
        &self,
        future: impl Future<Output = std::result::Result<(), String>> + Send + 'static,
    ) {
        let mut tasks = self.tasks.lock().expect("retained owner lock");
        reap(
            &mut tasks,
            &mut Context::from_waker(futures_util::task::noop_waker_ref()),
        );
        if tasks.sealed {
            // Only the shared process deadline can revoke a promised handoff.
            // Drop its owned capabilities here; never start an unjoined task.
            drop(future);
            tasks.failures = tasks.failures.saturating_add(1);
            tasks.last_failure = Some("retained handoff after process drain closed".into());
            return;
        }
        // A pre-reserved attached generation can hand off during shutdown. The
        // permit keeps close() open until this task is registered and joined.
        tasks.jobs.push(self.runtime.spawn(future));
        self.changed.notify_one();
    }
    pub(crate) fn request_shutdown(&self) {
        let mut tasks = self.tasks.lock().expect("retained owner lock");
        if !tasks.closing {
            tasks.closing = true;
            self.stop
                .send_replace(Some(TokioInstant::now() + self.grace));
        }
        self.changed.notify_one();
    }
    pub(crate) async fn close(&self) -> std::result::Result<(), String> {
        self.request_shutdown();
        let deadline = self.stop.borrow().expect("shutdown deadline");
        // Handles never leave the owner. Cancelling this future cannot detach
        // them, and late promised handoffs join the same absolute deadline.
        let drained = std::future::poll_fn(|cx| {
            let mut tasks = self.tasks.lock().expect("retained owner lock");
            reap(&mut tasks, cx);
            if tasks.jobs.is_empty() && self.capacity.used() == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        tokio::pin!(drained);
        loop {
            tokio::select! {
                biased;
                _=&mut drained=>break,
                _=self.changed.notified()=>{},
                _=tokio::time::sleep_until(deadline)=>{
                    let mut tasks=self.tasks.lock().expect("retained owner lock");
                    tasks.sealed=true;
                    for job in &tasks.jobs { job.abort(); }
                    tasks.failures=tasks.failures.saturating_add(1);
                    tasks.last_failure=Some("retained session drain timed out".into());
                    break;
                },
            }
        }
        // Abort is followed by join. Join handles remain resource-owned if this
        // final join is itself cancelled; Drop also aborts every outstanding job.
        std::future::poll_fn(|cx| {
            let mut tasks = self.tasks.lock().expect("retained owner lock");
            reap(&mut tasks, cx);
            if tasks.jobs.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let mut tasks = self.tasks.lock().expect("retained owner lock");
        tasks.sealed = true;
        if tasks.failures == 0 {
            Ok(())
        } else {
            Err(format!(
                "{} retained session failures: {}",
                tasks.failures,
                tasks.last_failure.as_deref().unwrap_or("unknown")
            ))
        }
    }
}
impl Drop for RetainedSessions {
    fn drop(&mut self) {
        for job in &self.tasks.get_mut().expect("retained owner lock").jobs {
            job.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn finished_jobs_do_not_timeout_when_runtime_shutdown_finishes_after_deadline() {
        let owner = RetainedSessions::new(1, Duration::from_millis(10));
        let permit = owner.reserve().unwrap();
        owner.spawn(async move {
            drop(permit);
            Ok(())
        });
        owner.request_shutdown();
        tokio::time::sleep(Duration::from_millis(30)).await;
        owner.close().await.unwrap();
    }
    #[tokio::test]
    async fn promised_handoff_is_bounded_and_shutdown_joins_it() {
        let owner = RetainedSessions::new(1, Duration::from_secs(1));
        let permit = owner.reserve().unwrap();
        assert!(owner.reserve().is_err());
        let closing = owner.close();
        tokio::pin!(closing);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut closing)
                .await
                .is_err()
        );
        let mut stop = owner.subscribe();
        assert!(stop.borrow().is_some());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = ran.clone();
        owner.spawn(async move {
            let _permit = permit;
            if stop.borrow().is_none() {
                stop.changed().await.unwrap();
            }
            observed.store(true, Ordering::SeqCst);
            Ok(())
        });
        closing.await.unwrap();
        assert!(ran.load(Ordering::SeqCst));
        assert_eq!(owner.capacity.used(), 0);
    }
    #[tokio::test]
    async fn cancelling_close_preserves_join_ownership_and_reports_task_panic() {
        let owner = RetainedSessions::new(1, Duration::from_secs(1));
        let permit = owner.reserve().unwrap();
        let gate = Arc::new(Notify::new());
        let waiting = gate.clone();
        owner.spawn(async move {
            let _permit = permit;
            waiting.notified().await;
            panic!("retained owner test panic");
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), owner.close())
                .await
                .is_err()
        );
        assert_eq!(
            owner.tasks.lock().unwrap().jobs.len(),
            1,
            "close cancellation detached a task"
        );
        gate.notify_one();
        let error = owner.close().await.unwrap_err();
        assert!(error.contains("retained owner test panic"));
        assert_eq!(owner.capacity.used(), 0);
        assert!(owner.tasks.lock().unwrap().jobs.is_empty());
    }
    #[tokio::test]
    async fn shutdown_deadline_aborts_joins_and_refuses_late_tasks() {
        let owner = RetainedSessions::new(2, Duration::from_millis(20));
        let permit = owner.reserve().unwrap();
        let late = owner.reserve().unwrap();
        owner.spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
            Ok(())
        });
        assert!(
            owner
                .close()
                .await
                .unwrap_err()
                .contains("retained session")
        );
        assert!(owner.tasks.lock().unwrap().jobs.is_empty());
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = ran.clone();
        owner.spawn(async move {
            let _permit = late;
            observed.store(true, Ordering::SeqCst);
            Ok(())
        });
        assert_eq!(owner.capacity.used(), 0);
        assert!(!ran.load(Ordering::SeqCst));
        assert!(owner.tasks.lock().unwrap().jobs.is_empty());
    }
}
