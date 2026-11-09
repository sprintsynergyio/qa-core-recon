# Releasing

These steps publish `qa-core-recon` 0.1.0 to PyPI. The publish workflow uses a Trusted Publisher. It does not use an API token. What this version ships, and what it leaves out, is in `docs/release-acceptance.md`.

## Check The Project Name

Open <https://pypi.org/project/qa-core-recon/>.

The name is free when that page says the project does not exist. If a project with that name is already there, stop. Do not point this workflow at a different name.

## Add A Pending Trusted Publisher

Sign in to pypi.org. Open the account menu, then Publishing, and add a pending publisher with these values:

| Field | Value |
| ----- | ----- |
| PyPI project name | `qa-core-recon` |
| Owner | `sprintsynergyio` |
| Repository name | `qa-core-recon` |
| Workflow name | `publish.yml` |
| Environment name | `pypi` |

The pending publisher accepts uploads only from that GitHub workflow in that environment.

## Create The GitHub Environment

In the GitHub repository, open Settings, then Environments, and create an environment named `pypi`.

You can add a required reviewer on that environment. The workflow then waits for approval before it publishes. Leave the rule off when the release should publish as soon as the workflow starts.

## Create Release v0.1.0

Create the release from `main`:

1. Open Releases, then Draft a new release.
2. Set the tag to `v0.1.0` and create that tag from `main`.
3. Set the release title to `v0.1.0`.
4. Publish the release.

The publish workflow runs when a GitHub release is published, and when a tag named `v*` is pushed. It does not run on a pull request or on a push to a branch.

## Watch The Workflow

Open the Actions tab and watch the Publish workflow for that release.

The workflow builds the sdist and the wheel, then publishes them with `pypa/gh-action-pypi-publish`. A green run means PyPI accepted the files.

## Verify The Install

Use a new virtual environment:

```bash
python3.11 -m venv /tmp/qa-core-recon-verify
source /tmp/qa-core-recon-verify/bin/activate
pip install qa-core-recon==0.1.0
qa-core-recon --version
```

The version line is `qa-core-recon 0.1.0`.

## Yank A Release

A yank tells installers to skip that version unless the user pins it. The files stay on PyPI. `pip install qa-core-recon==0.1.0` still works after a yank. `pip install qa-core-recon` moves on to a later version.

To yank 0.1.0:

1. Sign in to pypi.org as an owner of `qa-core-recon`.
2. Open the project, then the 0.1.0 release.
3. Choose Yank, and write a short reason.

This repository does not store a PyPI token, so the yank is done on the website. Do not delete the files from the publish workflow. PyPI will not accept another upload of version 0.1.0. Publish a new version when users need a fixed package.
