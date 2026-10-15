import os
import subprocess

import pytest
from git import GitCommandError

from metrics_layer.core.parse import github_repo
from metrics_layer.core.parse.github_repo import GithubRepo


def _git(*args, cwd):
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def origin_url(tmp_path, monkeypatch):
    """A bare origin with 3 commits on main and a second branch, cloned through file:// so that
    git honours --depth (it ignores it for plain local paths)."""
    monkeypatch.setattr(github_repo, "BASE_PATH", str(tmp_path / "clones"))
    (tmp_path / "clones").mkdir()
    work = tmp_path / "work"
    work.mkdir()
    _git("init", "-b", "main", cwd=work)
    for index in range(3):
        (work / f"file_{index}.yml").write_text(f"name: {index}\n")
        _git("add", ".", cwd=work)
        _git("commit", "-m", f"commit {index}", cwd=work)
    _git("checkout", "-b", "other", cwd=work)
    (work / "other_only.yml").write_text("name: other\n")
    _git("add", ".", cwd=work)
    _git("commit", "-m", "on other", cwd=work)
    origin = tmp_path / "origin.git"
    _git("clone", "--bare", str(work), str(origin), cwd=tmp_path)
    return f"file://{origin}"


def _remote_branches(repo):
    output = repo.git_repo.git.for_each_ref("--format=%(refname)", "refs/remotes/origin")
    names = [line.split("refs/remotes/origin/")[1] for line in output.splitlines() if line]
    return sorted(name for name in names if name != "HEAD")


def test_fetch_clones_every_branch_with_full_history_by_default(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch()
    try:
        assert int(repo.git_repo.git.rev_list("--count", "HEAD")) == 3
        assert _remote_branches(repo) == ["main", "other"]
        assert not os.path.exists(os.path.join(repo.repo_destination, ".git", "shallow"))
    finally:
        repo.delete()


def test_fetch_with_depth_clones_only_that_many_commits_of_the_branch(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch(depth=1)
    try:
        assert int(repo.git_repo.git.rev_list("--count", "HEAD")) == 1
        assert os.path.exists(os.path.join(repo.repo_destination, ".git", "shallow"))
        # Only the requested branch is fetched.
        assert "other" not in _remote_branches(repo)
        assert os.path.exists(os.path.join(repo.repo_destination, "file_2.yml"))
    finally:
        repo.delete()


def test_fetch_with_depth_still_lists_the_remote_branches(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch(depth=1)
    try:
        assert "other" in repo.branch_options and "main" in repo.branch_options
    finally:
        repo.delete()


def test_fetch_with_depth_of_a_missing_branch_fails_like_a_full_clone(origin_url):
    repo = GithubRepo(origin_url, "no-such-branch")
    with pytest.raises(GitCommandError) as raised:
        repo.fetch(depth=1)
    text = str(raised.value).lower()
    assert "remote branch" in text and "not found" in text


def test_clone_options_leave_the_clone_unchanged_without_a_depth():
    assert GithubRepo._clone_options() == {}
    assert GithubRepo._clone_options(None) == {}
    assert GithubRepo._clone_options(1) == {"depth": 1}
