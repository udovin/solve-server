use std::fs::{remove_dir_all, File};
use std::path::PathBuf;
use std::time::Duration;

use sbox::{run_as_root, BinNewIdMapper, Gid, Uid};
use solve::core::Error;
use solve::invoker::safeexec;
use tar::Archive;

mod common;

fn get_rootfs() -> Result<Archive<File>, Error> {
    let mut child = std::process::Command::new("/bin/sh")
        .arg("./get_rootfs.sh")
        .current_dir("./tests")
        .spawn()
        .unwrap();
    assert!(child.wait().unwrap().success());
    let mut rootfs = Archive::new(File::open("./tests/rootfs.tar")?);
    rootfs.set_preserve_permissions(true);
    rootfs.set_preserve_ownerships(true);
    rootfs.set_unpack_xattrs(true);
    Ok(rootfs)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_safeexec() {
    let tmpdir = common::temp_dir().unwrap();
    let rootfs_dir = tmpdir.join("rootfs");
    let user_mapper = BinNewIdMapper::new_root_subid(Uid::current(), Gid::current()).unwrap();
    {
        let rootfs_dir = rootfs_dir.clone();
        let mut rootfs = get_rootfs().unwrap();
        run_as_root(&user_mapper, move || Ok(rootfs.unpack(rootfs_dir)?)).unwrap();
    }
    let cgroup = match std::env::var("TEST_CGROUP_PATH") {
        Ok(v) => PathBuf::from(v)
            .strip_prefix("/sys/fs/cgroup")
            .unwrap()
            .to_owned(),
        Err(_) => PathBuf::from("solve-test-safeexec"),
    };
    let manager = safeexec::Manager::new(tmpdir.join("safeexec"), cgroup).unwrap();
    let config = safeexec::ProcessConfig {
        layers: vec![rootfs_dir.clone()],
        command: vec![
            "/bin/sh".into(),
            "-c".into(),
            "sleep 1 && echo -n 'solve_test'".into(),
        ],
        time_limit: Duration::from_secs(2),
        real_time_limit: Duration::from_secs(4),
        memory_limit: 1024 * 1024,
        ..Default::default()
    };
    let mut process = manager.create_process(config).unwrap();
    process.start().await.unwrap();
    let report = process.wait().await.unwrap();
    assert_eq!(report.exit_code, 0);
    // assert!(report.memory > 0);
    // assert!(report.time > Duration::ZERO);
    assert!(report.real_time > Duration::ZERO);
    run_as_root(&user_mapper, move || Ok(remove_dir_all(rootfs_dir)?)).unwrap();
}
