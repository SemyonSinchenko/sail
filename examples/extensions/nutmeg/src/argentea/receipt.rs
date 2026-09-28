//! One preformatted write per bounded receipt. A process-local stderr lock alone
//! cannot protect prefix/body fragments from another process sharing the log.
use std::io::{self, Write};
const MAX_LINE_BYTES: usize = 4096;

pub(super) fn write_line(writer: &mut impl Write, prefix: &str, json: &str) -> io::Result<()> {
    let bytes = prefix
        .len()
        .checked_add(json.len())
        .and_then(|n| n.checked_add(1));
    if bytes.is_none_or(|n| n > MAX_LINE_BYTES) || json.contains(['\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "receipt exceeds line bound or contains a raw newline",
        ));
    }
    let mut line = String::with_capacity(bytes.expect("validated line bound"));
    line.push_str(prefix);
    line.push_str(json);
    line.push('\n');
    writer.write_all(line.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        process::{Child, Command, Stdio},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    #[derive(Default)]
    struct Writes(Vec<Vec<u8>>);
    impl Write for Writes {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.push(bytes.to_vec());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn one_complete_write_contains_prefix_json_and_newline() {
        for prefix in ["ARGENTEA_RECEIPT ", ""] {
            let mut writes = Writes::default();
            write_line(&mut writes, prefix, r#"{"event":"close"}"#).unwrap();
            assert_eq!(
                writes.0,
                vec![format!("{prefix}{{\"event\":\"close\"}}\n").into_bytes()]
            );
        }
        for invalid in ["{}\n{}".to_string(), "x".repeat(MAX_LINE_BYTES)] {
            let mut writes = Writes::default();
            assert!(write_line(&mut writes, "ARGENTEA_RECEIPT ", &invalid).is_err());
            assert!(writes.0.is_empty());
        }
    }
    struct Children(Vec<Child>);
    impl Drop for Children {
        fn drop(&mut self) {
            for child in &mut self.0 {
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                }
                let _ = child.wait();
            }
        }
    }
    #[test]
    fn child_writes_complete_receipts() {
        let Ok(root) = std::env::var("SAIL_RECEIPT_TEST_DIRECTORY") else {
            return;
        };
        let writer: usize = std::env::var("SAIL_RECEIPT_TEST_WRITER")
            .unwrap()
            .parse()
            .unwrap();
        let root = std::path::Path::new(&root);
        fs::write(root.join(format!("ready-{writer}")), b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("start").exists() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        for sequence in 0..128 {
            let json=serde_json::to_string(&serde_json::json!({"writer":writer,"sequence":sequence,"event":"close","padding":"x".repeat(256)})).unwrap();
            write_line(&mut std::io::stderr().lock(), "ARGENTEA_RECEIPT ", &json).unwrap();
        }
    }
    #[test]
    fn eight_processes_append_only_whole_parseable_receipts() {
        let root = std::env::temp_dir().join(format!(
            "sail-receipts-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let log = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(root.join("receipts.log"))
            .unwrap();
        let mut children = Children(vec![]);
        for writer in 0..8 {
            children.0.push(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "argentea::receipt::tests::child_writes_complete_receipts",
                        "--nocapture",
                    ])
                    .env("SAIL_RECEIPT_TEST_DIRECTORY", &root)
                    .env("SAIL_RECEIPT_TEST_WRITER", writer.to_string())
                    .stdout(Stdio::null())
                    .stderr(log.try_clone().unwrap())
                    .spawn()
                    .unwrap(),
            );
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while (0..8).any(|i| !root.join(format!("ready-{i}")).exists()) {
            assert!(
                Instant::now() < deadline,
                "receipt child failed to become ready"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        fs::write(root.join("start"), b"start").unwrap();
        while children
            .0
            .iter_mut()
            .any(|child| child.try_wait().unwrap().is_none())
        {
            assert!(Instant::now() < deadline, "receipt child failed to finish");
            std::thread::sleep(Duration::from_millis(1));
        }
        for child in &mut children.0 {
            assert!(child.wait().unwrap().success());
        }
        let log = fs::read_to_string(root.join("receipts.log")).unwrap();
        let mut seen = [[false; 128]; 8];
        assert_eq!(log.lines().count(), 8 * 128);
        for line in log.lines() {
            let json = line
                .strip_prefix("ARGENTEA_RECEIPT ")
                .expect("one complete prefix");
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            let writer = value["writer"].as_u64().unwrap() as usize;
            let sequence = value["sequence"].as_u64().unwrap() as usize;
            assert!(!seen[writer][sequence]);
            seen[writer][sequence] = true;
        }
        assert!(seen.iter().flatten().all(|v| *v));
        drop(children);
        fs::remove_dir_all(root).unwrap();
    }
}
