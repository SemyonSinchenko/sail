use std::sync::Mutex;

use log::{Level, LevelFilter, Log, Metadata, Record};
use tonic::Code;

use super::*;

struct Capture(Mutex<Vec<String>>);

impl Log for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Warn
    }

    #[expect(
        clippy::unwrap_used,
        reason = "a poisoned capture mutex fails this test"
    )]
    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture(Mutex::new(Vec::new()));

#[derive(Debug)]
struct Outer(Status);

impl fmt::Display for Outer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Real error wrappers can print a source using Debug from Display.
        write!(f, "opaque outer error {:?}", self.0)
    }
}

impl Error for Outer {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[test]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "fixed valid fixtures and required captured evidence must fail the test if absent"
)]
fn failure_log_preserves_peer_key_original_status_and_nested_cause() {
    log::set_logger(&CAPTURE).unwrap();
    log::set_max_level(LevelFilter::Warn);
    let mut status = Status::with_details(
        Code::Unavailable,
        "injected body failure",
        bytes::Bytes::from_static(b"private-binary-details"),
    );
    status
        .metadata_mut()
        .insert("private-header", "private-header-value".parse().unwrap());
    status.set_source(std::sync::Arc::new(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "socket closed during body",
    )));
    let error = Outer(status);
    log_failure(
        format_args!(
            "event=flight_client_error phase=body peer=http://127.0.0.1:50051 job_id=7 stage=2 partition=3 attempt=0 channel=4"
        ),
        &error,
    );
    let records = CAPTURE.0.lock().unwrap();
    let record = records
        .iter()
        .find(|record| record.contains("injected body failure"))
        .expect("failure should emit a warning");
    for expected in [
        "execution_failure pid=",
        "phase=body",
        "peer=http://127.0.0.1:50051",
        "job_id=7 stage=2 partition=3 attempt=0 channel=4",
        "injected body failure",
        "caused_by=",
        "grpc_code=Unavailable",
        "socket closed during body",
    ] {
        assert!(record.contains(expected), "missing {expected:?}: {record}");
    }
    assert!(!record.contains("private-header"));
    assert!(!record.contains("private-binary-details"));
    drop(records);
    // Only real codec failures add a codec warning after the body boundary.
    log_codec_failure(
        format_args!("codec-passthrough"),
        &FlightError::Tonic(Box::new(error.0.clone())),
    );
    log_codec_failure(
        format_args!("codec-genuine"),
        &FlightError::DecodeError("bad IPC".into()),
    );
    let records = CAPTURE.0.lock().unwrap();
    assert!(
        !records
            .iter()
            .any(|record| record.contains("codec-passthrough"))
    );
    assert!(
        records
            .iter()
            .any(|record| record.contains("codec-genuine") && record.contains("bad IPC"))
    );
    // Diagnostics borrow the error; callers still receive the original status.
    assert_eq!(error.0.code(), Code::Unavailable);
    assert_eq!(error.0.message(), "injected body failure");
}

#[derive(Debug)]
struct Cyclic;

impl fmt::Display for Cyclic {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::panic::resume_unwind(Box::new("cyclic wrappers must not be formatted"))
    }
}

impl Error for Cyclic {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self)
    }
}

#[test]
fn cyclic_error_sources_cannot_block_failure_reporting() {
    let chain = ErrorChain(&Cyclic).to_string();
    assert_eq!(chain.matches("[wrapper omitted]").count(), 32);
    assert!(chain.ends_with("[source chain truncated after 32 errors]"));
}

#[test]
fn multibyte_status_and_task_fields_are_bounded_single_line_utf8() {
    let message = "漢🙂\n\r\t".repeat(4096);
    let status = Outer(Status::internal(message.clone()));
    let chain = ErrorChain(&status).to_string();
    let field = bounded_text(message);
    for text in [chain, field] {
        assert!(text.len() <= MAX_TEXT_BYTES);
        assert!(text.ends_with(TRUNCATED));
        assert!(text.contains("漢🙂\\n\\r\\t"));
        assert!(!text.chars().any(char::is_control));
    }
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "the fixed header value is valid test data"
)]
fn direct_status_omits_metadata_and_details_without_losing_its_source() {
    let mut status = Status::with_details(
        Code::Internal,
        "body failure",
        bytes::Bytes::from_static(b"secret-details"),
    );
    status
        .metadata_mut()
        .insert("secret-header", "secret-value".parse().unwrap());
    status.set_source(std::sync::Arc::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "keep-alive timed out",
    )));
    let chain = ErrorChain(&status).to_string();
    assert!(
        chain.contains("grpc_code=Internal message=body failure caused_by=keep-alive timed out")
    );
    assert!(!chain.contains("secret"));
}
