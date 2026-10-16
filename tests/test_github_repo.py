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


def _origin_sha(origin_url, ref):
    return subprocess.run(
        ["git", "rev-parse", ref],
        cwd=origin_url.removeprefix("file://"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_fetch_with_depth_alone_still_clones_the_branch_checked_out(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch(depth=1)
    try:
        assert not repo.git_repo.head.is_detached
        assert repo.git_repo.active_branch.name == "main"
    finally:
        repo.delete()


def test_fetch_by_ref_takes_a_branch_checked_out_detached(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch(depth=1, by_ref=True)
    try:
        assert repo.git_repo.head.is_detached
        assert repo.git_repo.head.commit.hexsha == _origin_sha(origin_url, "main")
        assert int(repo.git_repo.git.rev_list("--count", "HEAD")) == 1
        assert "other" in repo.branch_options and "main" in repo.branch_options
    finally:
        repo.delete()


def test_fetch_by_ref_takes_a_full_commit_sha(origin_url):
    sha = _origin_sha(origin_url, "main~2")
    repo = GithubRepo(origin_url, sha)
    repo.fetch(depth=1, by_ref=True)
    try:
        assert repo.git_repo.head.commit.hexsha == sha
        assert int(repo.git_repo.git.rev_list("--count", "HEAD")) == 1
        assert os.path.exists(os.path.join(repo.repo_destination, "file_0.yml"))
        assert not os.path.exists(os.path.join(repo.repo_destination, "file_2.yml"))
    finally:
        repo.delete()


def test_fetch_by_ref_takes_a_sha_that_is_not_on_the_default_branch(origin_url):
    sha = _origin_sha(origin_url, "other")
    repo = GithubRepo(origin_url, sha)
    repo.fetch(depth=1, by_ref=True)
    try:
        assert os.path.exists(os.path.join(repo.repo_destination, "other_only.yml"))
    finally:
        repo.delete()


def test_fetch_by_ref_takes_a_tag(origin_url):
    _git("tag", "v1", "main~1", cwd=origin_url.removeprefix("file://"))
    repo = GithubRepo(origin_url, "v1")
    repo.fetch(depth=1, by_ref=True)
    try:
        assert repo.git_repo.head.commit.hexsha == _origin_sha(origin_url, "main~1")
    finally:
        repo.delete()


def test_fetch_by_ref_without_a_depth_keeps_the_history(origin_url):
    repo = GithubRepo(origin_url, "main")
    repo.fetch(by_ref=True)
    try:
        assert int(repo.git_repo.git.rev_list("--count", "HEAD")) == 3
    finally:
        repo.delete()


@pytest.mark.parametrize(
    "ref, message", [("no-such-branch", "couldn't find remote ref"), ("0" * 40, "not our ref")]
)
def test_fetch_by_ref_of_a_missing_ref_fails(origin_url, ref, message):
    # git words a missing name and a missing sha differently.
    repo = GithubRepo(origin_url, ref)
    with pytest.raises(GitCommandError) as raised:
        repo.fetch(depth=1, by_ref=True)
    assert message in str(raised.value).lower()


def test_fetch_by_ref_of_an_abbreviated_sha_fails_as_a_missing_ref(origin_url):
    # A fetch resolves only full shas, so a caller holding an abbreviation has to expand it first.
    repo = GithubRepo(origin_url, _origin_sha(origin_url, "main")[:10])
    with pytest.raises(GitCommandError) as raised:
        repo.fetch(depth=1, by_ref=True)
    assert "couldn't find remote ref" in str(raised.value).lower()


def test_fetch_by_ref_does_not_read_a_ref_that_starts_with_a_dash_as_an_option(origin_url, tmp_path):
    marker = tmp_path / "ran"
    repo = GithubRepo(origin_url, f"--upload-pack=touch {marker}; git-upload-pack")
    with pytest.raises(GitCommandError):
        repo.fetch(depth=1, by_ref=True)
    assert not marker.exists()


def test_fetch_by_ref_does_not_read_a_ref_with_a_colon_as_a_refspec(origin_url):
    repo = GithubRepo(origin_url, "main:refs/heads/injected")
    with pytest.raises(GitCommandError) as raised:
        repo.fetch(depth=1, by_ref=True)
    assert "invalid ref name" in str(raised.value).lower()
    assert not os.path.exists(repo.repo_destination)


@pytest.mark.parametrize("ref", ["no-such-branch", "-bad"])
def test_a_failed_fetch_by_ref_leaves_no_directory_behind(origin_url, ref):
    repo = GithubRepo(origin_url, ref)
    with pytest.raises(GitCommandError):
        repo.fetch(depth=1, by_ref=True)
    assert not os.path.exists(repo.repo_destination)
