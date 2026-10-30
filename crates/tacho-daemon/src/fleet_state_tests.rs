use super::*;

#[test]
fn corrupt_state_is_refused_without_reopening_worker_slots(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = FleetStore::open(&home.path().join("events.sqlite"))?;
    for invalid in ["{invalid json", "{\"max_workers\":\"private-value\"}"] {
        store.conn.execute(
            "INSERT OR REPLACE INTO fleet_state(id,updated_at,state_json) VALUES(1,0,?1)",
            [invalid],
        )?;
        let error = store
            .load()
            .err()
            .ok_or_else(|| io::Error::other("invalid state was accepted"))?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!error.to_string().contains("private-value"));
        let stored: String =
            store
                .conn
                .query_row("SELECT state_json FROM fleet_state WHERE id=1", [], |r| {
                    r.get(0)
                })?;
        assert_eq!(stored, invalid);
    }
    Ok(())
}

#[test]
fn a_mutation_never_replaces_corrupt_state_or_runs_its_action(
) -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let mut store = FleetStore::open(&home.path().join("events.sqlite"))?;
    let invalid = "{invalid json";
    store.conn.execute(
        "INSERT INTO fleet_state(id,updated_at,state_json) VALUES(1,0,?1)",
        [invalid],
    )?;
    let called = std::cell::Cell::new(false);
    let result = store.mutate(None, |_| called.set(true));
    assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidData));
    assert!(!called.get());
    let stored: String =
        store
            .conn
            .query_row("SELECT state_json FROM fleet_state WHERE id=1", [], |r| {
                r.get(0)
            })?;
    assert_eq!(stored, invalid);
    Ok(())
}
