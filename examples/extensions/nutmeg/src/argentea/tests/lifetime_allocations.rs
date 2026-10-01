//! Test-only lifetime tracking for exactly-sized raw/CSR/state vertex buffers.
//! Thread-local current-thread async controls exclude prebuilt Arrow fixtures.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::argentea) struct Counts {
    wanted: usize,
    pointers: [usize; 16],
    pub calls: usize,
    pub peak: usize,
    overflow: bool,
}
thread_local! { static ACTIVE: Cell<Option<Counts>> = const { Cell::new(None) }; }
fn event(remove: usize, add: usize, bytes: usize) {
    let _ = ACTIVE.try_with(|active| {
        if let Some(mut c) = active.get() {
            if remove != 0 {
                for pointer in &mut c.pointers {
                    if *pointer == remove {
                        *pointer = 0;
                    }
                }
            }
            if add != 0 && bytes == c.wanted {
                c.calls += 1;
                if let Some(pointer) = c.pointers.iter_mut().find(|p| **p == 0) {
                    *pointer = add;
                } else {
                    c.overflow = true;
                }
                c.peak = c.peak.max(c.pointers.iter().filter(|p| **p != 0).count());
            }
            active.set(Some(c));
        }
    });
}
struct Tracked;
#[global_allocator]
static ALLOCATOR: Tracked = Tracked;
// SAFETY: Every request delegates to System with its original layout/pointer.
// Recording uses allocation-free thread-local Cells and a fixed pointer array.
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        event(0, p as usize, layout.size());
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        event(0, p as usize, layout.size());
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        event(p as usize, 0, 0);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        let next = unsafe { System.realloc(p, layout, bytes) };
        if !next.is_null() {
            event(p as usize, next as usize, bytes);
        }
        next
    }
}
pub(in crate::argentea) async fn measure<F: Future>(
    wanted: usize,
    future: F,
) -> (F::Output, Counts) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(None));
        }
    }
    ACTIVE.with(|active| {
        assert!(active.get().is_none());
        active.set(Some(Counts {
            wanted,
            pointers: [0; 16],
            calls: 0,
            peak: 0,
            overflow: false,
        }));
    });
    let reset = Reset;
    let output = future.await;
    let counts = ACTIVE.with(|active| active.get().unwrap());
    drop(reset);
    assert!(
        !counts.overflow,
        "lifetime probe exhausted its fixed pointer slots"
    );
    (output, counts)
}
