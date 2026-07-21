use std::{fs, path::PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates directory")
        .parent()
        .expect("repository root")
        .to_path_buf()
}

fn read_repo_file(path: &str) -> String {
    fs::read_to_string(repo_root().join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

#[test]
fn docusaurus_uses_canonical_docs_and_current_version_policy() {
    let config = read_repo_file("docs-site/docusaurus.config.js");
    let package: serde_json::Value =
        serde_json::from_str(&read_repo_file("docs-site/package.json"))
            .expect("parse docs-site package manifest");

    assert_eq!(
        package["dependencies"]["@docusaurus/core"], "3.10.2",
        "keep the site on the reviewed Docusaurus release"
    );
    assert!(config.contains("path: '../docs'"));
    assert!(config.contains("routeBasePath: '/'"));
    assert!(config.contains("onBrokenLinks: 'throw'"));
    assert!(config.contains("markdown:"));
    assert!(config.contains("onBrokenMarkdownLinks: 'throw'"));

    let workflow = read_repo_file("docs/docs-site.md");
    assert!(workflow.contains("current"));
    assert!(workflow.contains("Cut released versions manually"));
}

#[test]
fn sidebar_only_lists_checked_in_public_guides() {
    let sidebar = read_repo_file("docs-site/sidebars.js");
    let docs = [
        "admin",
        "production-runtime",
        "read-routing",
        "sharding",
        "policy",
        "mirroring",
        "adaptive-ops",
        "benchmarking",
        "metrics",
        "kubernetes",
        "docs-site",
    ];

    for document in docs {
        assert!(repo_root()
            .join("docs")
            .join(format!("{document}.md"))
            .is_file());
        assert!(sidebar.contains(&format!("'{document}'")));
    }

    for document in ["testing", "regression"] {
        assert!(repo_root()
            .join("docs")
            .join(format!("{document}.md"))
            .is_file());
        assert!(
            sidebar.contains(&format!("'{document}'")),
            "sidebar must publish {document} documentation"
        );
    }
}

#[test]
fn docs_link_checkers_are_checked_in() {
    for script in [
        "scripts/docs/check-links.sh",
        "scripts/docs/check-links.ps1",
    ] {
        let contents = read_repo_file(script);
        assert!(!contents.is_empty(), "{script} must not be empty");
        assert!(
            contents.contains("docs"),
            "{script} must check canonical docs"
        );
    }
}
