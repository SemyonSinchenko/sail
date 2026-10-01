//! Track seeded live pointers and new same-size allocations without allocating.
use std::cell::Cell;
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Watch {
    bytes: usize,
    pointers: [usize; 32],
    pub live: usize,
    pub peak: usize,
    pub allocations: usize,
    overflow: bool,
}
thread_local! { static STATE: Cell<Option<Watch>> = const { Cell::new(None) }; }

pub(crate) fn event(remove: usize, add: usize, bytes: usize) {
    let _ = STATE.try_with(|state| {
        if let Some(mut watch) = state.get() {
            if remove != 0
                && let Some(slot) = watch.pointers.iter_mut().find(|p| **p == remove)
            {
                *slot = 0;
                watch.live -= 1;
            }
            if add != 0 && bytes == watch.bytes {
                watch.allocations += 1;
                if let Some(slot) = watch.pointers.iter_mut().find(|p| **p == 0) {
                    *slot = add;
                    watch.live += 1;
                    watch.peak = watch.peak.max(watch.live);
                } else {
                    watch.overflow = true;
                }
            }
            state.set(Some(watch));
        }
    });
    if add != 0 {
        let action = HOOK
            .try_with(|hook| {
                let mut hook = hook.borrow_mut();
                if hook.as_ref().is_some_and(|(size, _)| *size == bytes) {
                    hook.take().map(|(_, action)| action)
                } else {
                    None
                }
            })
            .ok()
            .flatten();
        // Release the TLS borrow before invoking: cancellation may itself
        // allocate. Removing the one-shot hook first prevents recursion.
        if let Some(action) = action {
            action();
        }
    }
}

type Hook = (usize, Box<dyn FnOnce()>);
thread_local! { static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) }; }
pub(crate) struct HookGuard;
impl Drop for HookGuard {
    fn drop(&mut self) {
        HOOK.with(|hook| {
            hook.borrow_mut().take();
        });
    }
}
pub(crate) fn on_next_size(bytes: usize, action: impl FnOnce() + 'static) -> HookGuard {
    HOOK.with(|hook| {
        assert!(hook.borrow().is_none());
        *hook.borrow_mut() = Some((bytes, Box::new(action)));
    });
    HookGuard
}

pub(crate) struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        STATE.with(|state| state.set(None));
    }
}
pub(crate) fn start(bytes: usize, pointers: &[usize]) -> Guard {
    assert!(bytes > 0 && pointers.len() <= 32 && !pointers.contains(&0));
    STATE.with(|state| {
        assert!(state.get().is_none());
        let mut watch = Watch {
            bytes,
            live: pointers.len(),
            peak: pointers.len(),
            ..Watch::default()
        };
        watch.pointers[..pointers.len()].copy_from_slice(pointers);
        state.set(Some(watch));
    });
    Guard
}
pub(crate) fn snapshot() -> Watch {
    STATE.with(|state| {
        let watch = state.get().unwrap();
        assert!(!watch.overflow, "allocation pointer inventory overflow");
        watch
    })
}
