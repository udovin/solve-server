mod common;

#[test]
fn test_parse_str() {
    std::env::set_var("TEST_CGROUP", "safeexec");
    let data = r#"{
        "db": {
            "driver": "sqlite",
            "options": {
                "path": {{ ":memory:" | json }}
            }
        },
        "invoker": {
            "workers": 4,
            "safeexec": {
                "path": "safeexec",
                "cgroup": {{ env "TEST_CGROUP" | json }}
            }
        },
        "server": {
            "host": {{ "0.0.0.0" | json }},
            "port": {{ 4242 | json }},
            "site_url": "http://localhost:4242"
        }
    }"#;
    let config = solve::config::parse_str(data).unwrap();
    let server = config.server.as_ref().unwrap();
    assert_eq!(server.host, "0.0.0.0");
    assert_eq!(server.port, 4242);
    assert_eq!(server.site_url, "http://localhost:4242");
    let invoker = config.invoker.as_ref().unwrap();
    let safeexec = invoker.safeexec.as_ref().unwrap();
    assert_eq!(safeexec.cgroup, "safeexec");
}

#[test]
fn test_parse_file() {
    let tmpdir = common::temp_dir().unwrap();
    let data = r#"{
        "db": {
            "driver": "sqlite",
            "options": {
                "path": {{ ":memory:" | json }}
            }
        }
    }"#;
    std::fs::write(tmpdir.join("config.json"), data).unwrap();
    solve::config::parse_file(tmpdir.join("config.json")).unwrap();
}
