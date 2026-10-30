use tacho_core::decide::plugin::{parse_gh_pr_list, CliForgePlugin, ForgePlugin};

#[test]
fn an_open_pull_observation_preserves_the_files_that_select_its_checks() {
    let forge = CliForgePlugin::new("owner/project");
    let argv = forge.plan_open_prs_cmd();
    let fields = argv.windows(2).find_map(|pair| {
        (pair.first().map(String::as_str) == Some("--json"))
            .then(|| pair.get(1))
            .flatten()
    });
    assert!(fields.is_some_and(|fields| fields.split(',').any(|field| field == "files")));
    assert!(fields.is_some_and(|fields| fields.split(',').any(|field| field == "changedFiles")));
    let pulls = parse_gh_pr_list(r#"[{"number":7,"headRefOid":"head","files":[{"path":"src/lib.rs"},{"path":"README.md"}]}]"#)
        .expect("valid forge output");
    assert_eq!(
        pulls.first().map(|pull| &pull.files),
        Some(&vec!["src/lib.rs".to_string(), "README.md".to_string()])
    );
}

#[test]
fn a_partial_forge_file_list_cannot_select_an_incomplete_proof() {
    let result = parse_gh_pr_list(
        r#"[{"number":7,"headRefOid":"head","changedFiles":2,"files":[{"path":"README.md"}]}]"#,
    );
    assert!(result.is_err());
}
