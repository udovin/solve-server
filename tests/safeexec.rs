use std::path::PathBuf;
use std::time::Duration;

use solve::invoker::safeexec;

mod common;

#[ignore]
#[test]
fn test_safeexec() {
    let tmpdir = common::temp_dir().unwrap();
    let alpine_dir = PathBuf::new();
    let manager =
        safeexec::Manager::new(tmpdir.join("safeexec"), "../solve-test-safeexec").unwrap();
    let config = safeexec::ProcessConfig {
        layers: vec![alpine_dir],
        command: vec![
            "/bin/sh".into(),
            "-c".into(),
            "sleep 1 && echo -n 'solve_test'".into(),
        ],
        time_limit: Duration::from_secs(2),
        memory_limit: 1024 * 1024,
        ..Default::default()
    };
    let mut process = manager.process(config).unwrap();
    process.start().unwrap();
    let report = process.wait().unwrap();
    assert_eq!(report.exit_code, 0);
    assert!(report.memory > 0);
    assert!(report.time > Duration::ZERO);
    assert!(report.real_time > Duration::ZERO);
}
