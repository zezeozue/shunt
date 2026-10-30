use std::collections::BTreeSet;
use tacho_core::decide::land::{stray_shots, ShotRules};

#[test]
fn the_project_example_preserves_screen_and_shared_code_ownership(
) -> Result<(), Box<dyn std::error::Error>> {
    let rules: ShotRules =
        serde_json::from_str(include_str!("../../../examples/nzela-shots.json"))?;
    let scenarios = [
        (
            "frontend/admin/src/screens/Home.tsx",
            "frontend/admin/screenshots/02-dashboard.png",
            false,
        ),
        (
            "frontend/admin/src/screens/Settings.tsx",
            "frontend/admin/screenshots/15-preferences.png",
            true,
        ),
        (
            "frontend/admin/src/screens/Providers.tsx",
            "frontend/admin/screenshots/14-treasury.png",
            false,
        ),
        (
            "frontend/admin/src/screens/Transfers.tsx",
            "frontend/admin/screenshots/07-senders.png",
            true,
        ),
        (
            "frontend/admin/src/components/Table.tsx",
            "frontend/admin/screenshots/07-senders.png",
            false,
        ),
        (
            "frontend/admin/src/components/Table.tsx",
            "frontend/android/docs/screenshots/Home.png",
            true,
        ),
        (
            "frontend/admin/e2e/home.spec.ts",
            "frontend/admin/screenshots/02-dashboard.png",
            true,
        ),
        (
            "frontend/android/app/src/main/java/app/nzela/ui/components/Money.kt",
            "frontend/android/docs/screenshots/Home.png",
            false,
        ),
        (
            "frontend/android/app/src/main/java/app/nzela/ui/calculator/Calculator.kt",
            "frontend/android/docs/screenshots/Calculator.png",
            false,
        ),
        (
            "frontend/android/app/src/main/java/app/nzela/ui/calculator/Calculator.kt",
            "frontend/android/docs/screenshots/Home.png",
            true,
        ),
        (
            "frontend/android/app/src/test/java/app/nzela/screenshot/CoreScreenshotTest.kt",
            "frontend/android/docs/screenshots/Home.png",
            false,
        ),
        (
            "frontend/android/app/src/test/java/app/nzela/core/MoneyTest.kt",
            "frontend/android/docs/screenshots/Home.png",
            true,
        ),
    ];
    for (source, shot, stray) in scenarios {
        let actual = stray_shots(&[source, shot], &BTreeSet::new(), &rules);
        assert_eq!(
            actual,
            if stray { vec![shot] } else { Vec::new() },
            "{source}"
        );
        assert!(
            stray_shots(&[source, shot], &BTreeSet::from([shot.to_string()]), &rules).is_empty()
        );
    }
    Ok(())
}
