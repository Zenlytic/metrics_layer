import os
import subprocess
import time

import git
import pytest

from metrics_layer.core.parse import github_repo
from metrics_layer.core.parse.github_repo import GitCloneTimeout, GithubRepo

BRANCHES = ["main", "feature-a", "feature-b", "feature-c"]


def _git(cwd, *args):
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    return subprocess.run(["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout


@pytest.fixture
def remote(tmp_path):
    """A bare repo with several branches, each with some history. Returns (path, commits per branch)."""
    work = tmp_path / "work"
    work.mkdir()
    _git(work, "init", "-q", "-b", "main")
    counts = {}
    for i, branch in enumerate(BRANCHES):
        if branch != "main":
            _git(work, "checkout", "-q", "-b", branch, "main")
        n = 3 + i
        for k in range(n):
            (work / f"{branch}.txt").write_text("x" * 5000 + os.urandom(2000).hex() + str(k))
            _git(work, "add", "-A")
            _git(work, "commit", "-q", "-m", f"{branch} {k}")
        counts[branch] = int(_git(work, "rev-list", "--count", "HEAD").strip())
    bare = tmp_path / "remote.git"
    _git(tmp_path, "clone", "-q", "--bare", str(work), str(bare))
    return str(bare), counts


@pytest.fixture
def base_path(tmp_path, monkeypatch):
    path = tmp_path / "clones"
    path.mkdir()
    monkeypatch.setattr(github_repo, "BASE_PATH", str(path))
    return path


def _remote_branches(repo):
    out = repo.git.for_each_ref("refs/remotes/origin/", "--format=%(refname)")
    return sorted(
        r.split("refs/remotes/origin/")[1] for r in out.split("\n") if r and not r.endswith("/HEAD")
    )


def _git_dir_size(repo):
    return sum(
        os.path.getsize(os.path.join(root, f)) for root, _, files in os.walk(repo.git_dir) for f in files
    )


def test_default_fetch_clones_all_branches(remote, base_path):
    url, _ = remote
    repo = GithubRepo(url, "feature-a")
    repo.fetch()

    assert isinstance(repo.git_repo, git.Repo)
    assert repo.dbt_path == repo.folder
    assert repo._file_path is None
    assert sorted(repo.branch_options) == sorted(BRANCHES)
    assert _remote_branches(repo.git_repo) == sorted(BRANCHES)
    assert repo.git_repo.active_branch.name == "feature-a"


def test_single_branch_fetch(remote, base_path):
    url, counts = remote
    full = GithubRepo(url, "feature-b")
    full.fetch()
    single = GithubRepo(url, "feature-b")
    single.fetch(single_branch=True)

    assert _remote_branches(single.git_repo) == ["feature-b"]
    assert single.git_repo.active_branch.name == "feature-b"
    assert int(single.git_repo.git.rev_list("--count", "HEAD")) == counts["feature-b"]
    assert counts["feature-b"] > 1
    assert _git_dir_size(single.git_repo) < _git_dir_size(full.git_repo)
    # the branch list still shows every branch on the remote
    assert sorted(single.branch_options) == sorted(BRANCHES)


def test_single_branch_missing_branch(remote, base_path):
    url, _ = remote
    repo = GithubRepo(url, "does-not-exist")
    with pytest.raises(git.GitCommandError) as exc_info:
        repo.fetch(single_branch=True)

    text = str(exc_info.value).lower()
    assert "remote branch" in text and "not found" in text
    assert not os.path.exists(repo.repo_destination)


def test_failed_clone_never_leaks_token(base_path, tmp_path):
    token = "ghs_FAKETOKEN123456"
    url = f"https://x-access-token:{token}@127.0.0.1:1/org/repo.git"
    for kwargs in ({"single_branch": True}, {"timeout_seconds": 20}):
        repo = GithubRepo(url, "main")
        with pytest.raises(git.GitCommandError) as exc_info:
            repo.fetch(**kwargs)
        assert token not in str(exc_info.value)
        assert token not in repr(exc_info.value)
        assert "x-access-token" not in str(exc_info.value)
        assert "<repository url>" in str(exc_info.value)


def _install_hanging_ssh(tmp_path, monkeypatch):
    """Put an `ssh` that just sleeps first on PATH, so any ssh clone hangs. Returns the pid file path."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pid_file = tmp_path / "ssh.pid"
    script = bin_dir / "ssh"
    script.write_text(f"#!/bin/sh\necho $$ > {pid_file}\nexec sleep 60\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return pid_file


def _process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_timeout_kills_clone_and_cleans_up(tmp_path, base_path, monkeypatch):
    pid_file = _install_hanging_ssh(tmp_path, monkeypatch)
    repo = GithubRepo("git@example.invalid:org/repo.git", "main")

    start = time.monotonic()
    with pytest.raises(GitCloneTimeout) as exc_info:
        repo.fetch(private_key=b"fake key", timeout_seconds=0.5)
    elapsed = time.monotonic() - start

    assert elapsed < 1.5
    assert isinstance(exc_info.value, git.GitCommandError)
    assert "timed out after 0.5 seconds" in str(exc_info.value)
    assert not os.path.exists(repo.repo_destination)
    assert os.listdir(base_path) == []  # destination and temp ssh key file are both gone

    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 3
    while _process_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _process_alive(pid)


def test_timeout_is_generous_enough_for_a_normal_clone(remote, base_path):
    url, _ = remote
    repo = GithubRepo(url, "main")
    repo.fetch(timeout_seconds=30)
    assert sorted(repo.branch_options) == sorted(BRANCHES)
    assert _remote_branches(repo.git_repo) == sorted(BRANCHES)
