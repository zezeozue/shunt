use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(super) struct BatchCheckout {
    repo: PathBuf,
    directory: Option<tempfile::TempDir>,
    path: PathBuf,
}

fn git(repo: &Path, args: &[&str]) -> io::Result<String> {
    tacho_daemon::sampler::run_git_in_worktree(repo, args, Duration::from_secs(30))
        .map(|output| output.trim().to_string())
}

impl BatchCheckout {
    pub(super) fn new(repo: &Path, main: &str) -> io::Result<Self> {
        let root = crate::kit::private_tmp_dir("batch-checkout")?;
        let directory = tempfile::tempdir_in(root)?;
        let path = directory.path().join("checkout");
        let destination = path
            .to_str()
            .ok_or_else(|| io::Error::other("batch path is not UTF-8"))?;
        git(repo, &["worktree", "add", "--detach", destination, main])?;
        Ok(Self {
            repo: repo.to_path_buf(),
            directory: Some(directory),
            path,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn prepare(&self, main: &str, heads: &[&str]) -> io::Result<String> {
        git(&self.path, &["reset", "--hard", main])?;
        git(&self.path, &["clean", "-ffdx"])?;
        for head in heads {
            git(&self.path, &["merge", "--no-ff", "--no-edit", head])?;
        }
        git(&self.path, &["rev-parse", "HEAD^{tree}"])
    }

    pub(super) fn verify_unchanged(&self, expected_tree: &str) -> io::Result<()> {
        if git(&self.path, &["rev-parse", "HEAD^{tree}"])? != expected_tree {
            return Err(io::Error::other(
                "checks changed the merged tree; a passing proof requires the recorded PR heads",
            ));
        }
        git(&self.path, &["diff", "--quiet", "HEAD"])
            .map(|_| ())
            .map_err(|_| io::Error::other("checks changed tracked files; collect generated artifacts into the PR heads before verification"))
    }
}

impl Drop for BatchCheckout {
    fn drop(&mut self) {
        let result = self
            .path
            .to_str()
            .ok_or_else(|| io::Error::other("batch path is not UTF-8"))
            .and_then(|path| git(&self.repo, &["worktree", "remove", "--force", path]));
        if let Err(error) = result {
            if let Some(directory) = self.directory.take() {
                let retained = directory.keep();
                eprintln!("Batch checkout retained at {}: {error}", retained.display());
            }
        }
    }
}
