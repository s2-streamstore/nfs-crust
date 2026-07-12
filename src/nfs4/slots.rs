use super::*;

#[derive(Debug)]
pub(super) struct SlotTable {
    slots: Vec<Mutex<SlotState>>,
    active: Vec<AtomicBool>,
    pub(super) available: Semaphore,
    live_slots: AtomicUsize,
    slot_limits: AtomicU64,
    state_changed: Notify,
}

impl SlotTable {
    pub(super) fn new(slot_count: u32) -> Self {
        let slot_count = slot_count.max(1);
        let slots = (0..slot_count)
            .map(|id| {
                Mutex::new(SlotState {
                    id,
                    sequenceid: 1,
                    usable: true,
                })
            })
            .collect();
        let highest_slotid = slot_count as usize - 1;
        Self {
            slots,
            active: (0..slot_count).map(|_| AtomicBool::new(false)).collect(),
            available: Semaphore::new(slot_count as usize),
            live_slots: AtomicUsize::new(slot_count as usize),
            slot_limits: AtomicU64::new(pack_slot_limits(highest_slotid, highest_slotid)),
            state_changed: Notify::new(),
        }
    }

    pub(super) fn effective_len(&self) -> usize {
        (self.target_highest_slotid() + 1).min(self.live_slots.load(Ordering::Acquire))
    }

    fn target_highest_slotid(&self) -> usize {
        self.slot_limits().1
    }

    fn slot_limits(&self) -> (usize, usize) {
        let limits = self.slot_limits.load(Ordering::Acquire);
        ((limits >> 32) as usize, limits as u32 as usize)
    }

    fn active_highest_slotid(&self) -> usize {
        self.active
            .iter()
            .rposition(|active| active.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Returns a highest-slot value that honors the server's latest target
    /// while still reporting any higher-numbered requests already in flight.
    pub(super) async fn request_highest_slotid(&self, acquired_slotid: u32) -> Option<u32> {
        loop {
            let changed = self.state_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (enforced_highest, target_highest) = self.slot_limits();
            let acquired_slotid = acquired_slotid as usize;
            if acquired_slotid > target_highest || acquired_slotid > enforced_highest {
                return None;
            }
            let active_highest = self.active_highest_slotid();
            if active_highest <= enforced_highest {
                return Some(active_highest.max(target_highest) as u32);
            }
            changed.await;
        }
    }

    pub(super) fn update_limits(&self, highest_slotid: u32, target_highest_slotid: u32) {
        let local_highest = self.slots.len() - 1;
        let enforced = (highest_slotid as usize).min(local_highest);
        let target = (target_highest_slotid as usize)
            .min(enforced)
            .min(local_highest);
        self.slot_limits
            .store(pack_slot_limits(enforced, target), Ordering::Release);
        self.state_changed.notify_waiters();
    }

    pub(super) async fn acquire(&self) -> Result<SlotGuard<'_>, Error> {
        loop {
            let permit = self
                .available
                .acquire()
                .await
                .map_err(|_| all_slots_unusable_error())?;
            let changed = self.state_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut eligible_slot_locked = false;
            for index in 0..=self.target_highest_slotid() {
                let slot = &self.slots[index];
                match slot.try_lock() {
                    Ok(guard) if guard.usable => {
                        self.active[index].store(true, Ordering::Release);
                        return Ok(SlotGuard {
                            guard,
                            available: &self.available,
                            live_slots: &self.live_slots,
                            release: SlotRelease {
                                permit: Some(permit),
                                active: &self.active[index],
                                state_changed: &self.state_changed,
                                retire_permit: false,
                            },
                            request_outstanding: false,
                        });
                    }
                    Ok(_) => {}
                    Err(_) => eligible_slot_locked = true,
                }
            }

            drop(permit);
            if !eligible_slot_locked {
                return Err(all_slots_unusable_error());
            }
            changed.await;
        }
    }

    pub(super) async fn acquire_for_request(&self) -> Result<(SlotGuard<'_>, u32), Error> {
        loop {
            let slot = self.acquire().await?;
            if let Some(highest_slotid) = self.request_highest_slotid(slot.id).await {
                return Ok((slot, highest_slotid));
            }
            // The server lowered its limit after this slot was selected but
            // before the request was issued. Dropping is safe because the
            // request is not armed yet; acquire again under the new target.
            drop(slot);
        }
    }
}

fn pack_slot_limits(enforced_highest_slotid: usize, target_highest_slotid: usize) -> u64 {
    ((enforced_highest_slotid as u64) << 32) | target_highest_slotid as u64
}

fn all_slots_unusable_error() -> Error {
    Error::connection_lost("all eligible NFSv4.1 session slots are unusable")
}

#[derive(Debug)]
pub(super) struct SlotState {
    pub(super) id: u32,
    pub(super) sequenceid: u32,
    pub(super) usable: bool,
}

pub(super) struct SlotGuard<'a> {
    // Field order is intentional: Rust drops the mutex guard before the
    // release notifier, so woken acquirers always observe an unlocked slot.
    guard: MutexGuard<'a, SlotState>,
    release: SlotRelease<'a>,
    available: &'a Semaphore,
    live_slots: &'a AtomicUsize,
    request_outstanding: bool,
}

struct SlotRelease<'a> {
    permit: Option<SemaphorePermit<'a>>,
    active: &'a AtomicBool,
    state_changed: &'a Notify,
    retire_permit: bool,
}

impl Deref for SlotGuard<'_> {
    type Target = SlotState;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for SlotGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl SlotGuard<'_> {
    pub(super) fn mark_request_outstanding(&mut self) {
        self.request_outstanding = true;
    }

    pub(super) fn disarm_request(&mut self) {
        self.request_outstanding = false;
    }

    pub(super) fn advance_sequence(&mut self) {
        self.sequenceid = self.sequenceid.wrapping_add(1);
    }

    pub(super) fn mark_unusable(&mut self) {
        if self.usable {
            self.usable = false;
            self.release.retire_permit = true;
            if self.live_slots.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.available.close();
            }
        }
    }
}

impl Drop for SlotGuard<'_> {
    fn drop(&mut self) {
        if self.request_outstanding {
            self.mark_unusable();
        }
    }
}

impl Drop for SlotRelease<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.state_changed.notify_waiters();
        if self.retire_permit
            && let Some(permit) = self.permit.take()
        {
            permit.forget();
        }
    }
}
