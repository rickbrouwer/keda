# Contributing to KEDA

Thanks for helping make KEDA better 😍.

There are many areas we can use contributions - ranging from code, documentation, feature proposals, issue triage, samples, and content creation.

This document is for contributing to KEDA. There is also an [`AGENTS.md`](AGENTS.md) that mirrors these rules for AI coding agents (Claude Code, Codex, Cursor, Copilot, Aider, etc.). Both files must be kept in sync; if you change one, update the other.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Project governance](#project-governance)
- [Getting Help](#getting-help)
- [Before you start: issue-first policy](#before-you-start-issue-first-policy)
- [Making Breaking Changes](#making-breaking-changes)
- [Contributing Scalers](#contributing-scalers)
  - [Testing](#testing)
- [Contributing webhooks](#contributing-webhooks)
- [Metrics and Logging](#metrics-and-logging)
  - [Metrics](#metrics)
  - [Logging and Log Messages](#logging-and-log-messages)
  - [Legacy](#legacy)
- [Required local checks before opening a PR](#required-local-checks-before-opening-a-pr)
- [Pull request rules](#pull-request-rules)
- [Scope discipline](#scope-discipline)
- [Changelog](#changelog)
- [Including Documentation Changes](#including-documentation-changes)
- [Creating and building a local environment](#creating-and-building-a-local-environment)
- [Developer Certificate of Origin: Signing your work](#developer-certificate-of-origin-signing-your-work)
  - [Every commit needs to be signed](#every-commit-needs-to-be-signed)
  - [Commit hygiene](#commit-hygiene)
  - [I didn't sign my commit, now what?!](#i-didnt-sign-my-commit-now-what)
- [Code Quality](#code-quality)
- [When in doubt](#when-in-doubt)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Project governance

You can learn about the governance of KEDA [here](https://github.com/kedacore/governance).

## Getting Help

If you have a question about KEDA or how best to contribute, the [#KEDA](https://kubernetes.slack.com/archives/CKZJ36A5D) channel on the Kubernetes slack channel ([get an invite if you don't have one already](https://slack.k8s.io/)) is a good place to start.  We also have regular [community stand-ups](https://github.com/kedacore/keda#community) to track ongoing work and discuss areas of contribution.  For any issues with the product you can [create an issue](https://github.com/kedacore/keda/issues/new) in this repo.

## Before you start: issue-first policy

We want your contribution to land, so please read this before opening a pull request. Drive-by PRs, speculative refactors, "found a typo" PRs, and unsolicited feature work are not accepted.

- **An issue must exist first.** Do not open a pull request unless there is an existing, open, and approved GitHub Issue that describes the work.
- **Ask to be assigned.** Do not start non-trivial work on an issue unless it is assigned to you. If an issue is unassigned, comment on it asking a maintainer to assign it to you, and wait for confirmation before writing code.
- **Respect claimed issues.** If someone has commented that they intend to work on the issue, or has been assigned to it, do not open a competing PR, push commits, or start a draft PR. If there has been no visible progress for an extended period, it is fine to politely ask whether they are still actively working on it before taking further action.
- **Stay inside the issue's scope.** Implement only what the issue describes. If you discover related problems, mention them in the PR description or open a separate issue rather than silently expanding the scope.
- **One issue, one PR.** Do not bundle multiple issues into a single PR, and do not split a single issue across multiple PRs without coordinating in the issue first.
- **Do not duplicate automation.** Dependency bumps (Renovate/Dependabot), changelog regeneration, generated-file refreshes, and similar housekeeping are handled by bots or release tooling. Do not open PRs that duplicate that work.

## Making Breaking Changes

All contributions must follow [our deprecation breaking changes policy](https://github.com/kedacore/governance/blob/main/DEPRECATIONS.md).

## Contributing Scalers

One of the easiest ways to contribute is adding scalers.  Scalers are the logic on when to activate a container (scaling from zero to one) and also how to serve metrics for an event source.  You can view [the code for existing scalers here](https://github.com/kedacore/keda/tree/main/pkg/scalers).  When writing a scaler, please consider:

1. Is this an event source that many others will access from Kubernetes? If not, potentially consider [creating an external scaler](https://github.com/kedacore/keda/blob/main/pkg/scalers/externalscaler/externalscaler.proto).
1. Provide tests
1. Provide [documentation and examples](https://github.com/kedacore/keda-docs#adding-scaler-documentation) for [keda.sh](https://keda.sh)

Information on how scalers work can be found in [`CREATE-NEW-SCALER`](CREATE-NEW-SCALER.md) and read our [scaler governance policy](https://github.com/kedacore/governance/blob/main/SCALERS.md).

Scalers in [`pkg/scaling/scalers_builder.go`](pkg/scaling/scalers_builder.go) must remain sorted alphabetically (enforced by [`tools/sort_scalers.sh`](tools/sort_scalers.sh)).

### Testing

It is mandatory to provide end-to-end (e2e) tests for new scalers. For more information on e2e testing in KEDA
check the [test documentation](./tests/README.md). Those tests are run nightly on our
[CI system](https://github.com/kedacore/keda/actions?query=workflow%3A%22nightly+e2e+test%22).

Additional testing rules:

- **Bug fixes should add a regression test** that fails without the fix.
- **New behavior in existing code should be covered by unit tests.**
- **Do not delete existing tests to make a build green.** If a test is genuinely wrong, explain why in the PR description.
- **Do not weaken assertions** (for example replacing exact checks with `assert.NotNil`) just to make a flaky test pass. Investigate the flake instead.

## Contributing webhooks

Another easy way to contribute is improving the validations to avoid misconfigurations. New rules can be added in the proper type's webhooks file (`apis/keda/v1alpha1/*_webhook.go`).

## Metrics and Logging

### Metrics

Incorporating Prometheus and OpenTelemetry metrics is essential in our project. When creating metrics, please consider the following guidelines:

- **Always specify the unit in the metric name using standard units** (e.g., use seconds instead of milliseconds, bytes instead of megabytes, etc.).
- **Choose descriptive metric names**. Instead of vague names like `message_number` or `triggers`, opt for more specific ones like `queued_messages` and `trigger_registered`.
- **Ensure consistency in metric naming**. Review existing metrics for their naming patterns and try to align new metrics accordingly.
- **Utilize labels for differentiating metric states**. Instead of creating separate metrics like `messages_sent_successfully` and `messages_sent_failed`, create a single metric `messages_sent` and differentiate using a label `state` with values `success` or `failed`.
- **Avoid overly detailed metrics**. Refrain from using labels with high cardinality (such as _email_, _message-id_, or _time_), as this can burden the system.
- **Favor metrics that are cumulative counters**. Users can then apply functions like `rate()` to calculate changes over time. Append `total` for Prometheus and `count` for OpenTelemetry metrics.
- **Provide clear descriptions**. This should tell end-users what the metrics represent, without being a KEDA expert nor technical person.

For further guidance on metric naming and labeling, refer to the recommendations in the [Prometheus](https://prometheus.io/docs/practices/naming/) and [OpenTelemetry](https://opentelemetry.io/docs/specs/semconv/general/metrics/) documentation.

### Logging and Log Messages

When adding log messages to the project, it's crucial to set the appropriate log level and tailor the message for its intended audience:
- Use `debug` level for KEDA project developers, who possess deep knowledge of the system's inner workings. Messages should be data-rich and detailed. In the code a debug message is written via verbosity level 1 on the `Info()` method on the logger, eg. `logger.V(1).Info(msg)`.
- Set to `info` level for engineers familiar with KEDA's components but not its intricate details. These messages should serve as updates or milestones regarding the system's sub-components, essentially acting as a status report. In the code an info message is written via `Info()` method on the logger, eg. `logger.Info(msg)`.
- `Warning` level and above is aimed at operational teams. Messages should be clear, concise, and either indicate consequences or be actionable, with suggestions for next steps. It indicates a problem which should be addressed in the near future if it persists. The message should contain at least the consequence. Include links to further documentation where possible. In the code a warning is written via the `Info()` method on the logger with a `"Warning:"` prefix in the message, eg. `logger.Info("Warning: ...msg")`.
- An `error` indicates a failure, and a part of the system is not capable of fulfilling its intended purpose. An error should contain what part failed, why, and possible solutions. In the code an error is usually written via the `Error()` method on the logger, eg. `logger.Error(err, msg)`.

### Legacy

Some of the metrics and log messages in the project don't follow the above practices, but are there for historical reasons. When refactoring pieces of code, please try to apply the best practices to any log message or metric which is impacted.

## Required local checks before opening a PR

Run these from the repo root and make sure they all pass before opening a PR. CI will run them too, but catching issues locally first saves review round-trips.

| Check | Command | When |
| --- | --- | --- |
| Code formatting | `make fmt` (wraps `go fmt`) | Always |
| Static analysis | `make vet` (wraps `go vet`) | Always |
| Linter | `make golangci` | Always |
| Unit tests | `make test` | Always |
| Generated code | `make generate` | When you change anything under `apis/`, mocks, or protobuf |
| Scalers schema | `make generate-scalers-schema` | When you add or change a scaler's metadata, fields, or annotations |
| Schema verification | `make verify-scalers-schema` | After running `generate-scalers-schema` |
| Manifest verification | `make verify-manifests` | When you change CRDs or RBAC |

Optionally install [pre-commit](https://pre-commit.com) and run `pre-commit run --all-files`. This executes `go fmt`, trailing-whitespace, end-of-file fixer, doctoc, `golangci-lint`, the scaler-sort check, and the changelog validator in one go. See [`.pre-commit-config.yaml`](.pre-commit-config.yaml).

**Do not** skip, disable, or bypass these checks (for example `--no-verify`, commenting out linters, adding broad `//nolint` directives) to make a PR pass. Fix the underlying issue.

## Pull request rules

- **Do not delete or modify the checklist** in [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md). When opening a PR, keep every checklist item and tick off only the boxes that genuinely apply to the change.
- Keep the `Fixes #` / `Relates to #` lines and fill them in when there is a related issue or PR.
- Write a clear description of *what* changed and *why*. Do not leave the template description empty.
- One logical change per PR. Do not bundle unrelated refactors with feature work or bug fixes.

## Scope discipline

- Do not "drive-by" reformat, rename, or restructure code outside the scope of the requested change.
- Do not bump dependencies unless the task requires it.
- Do not change CI workflows, release tooling, or governance files unless explicitly asked.
- If you encounter unrelated issues while working, mention them in the PR description rather than fixing them in the same PR.

## Changelog

Every user-visible change must be added to [`CHANGELOG.md`](CHANGELOG.md) under the `## Unreleased` section. The pre-commit hook [`hack/validate-changelog.sh`](hack/validate-changelog.sh) verifies this.

Rules:

- Place the entry under the correct subsection: `### New`, `### Improvements`, `### Fixes`, `### Deprecations`, `### Breaking Changes`, or `### Other`.
- Format: `- **<Scaler Name>**: <Description> ([#<ID>](https://github.com/kedacore/keda/issues/<ID>))`.
  - Use `**General**:` for cross-cutting changes; these go at the top of the subsection.
  - Otherwise use the scaler name (for example `**Kafka Scaler**:`).
- Entries are sorted **alphabetically** within each subsection, with `General` always first.
- `<ID>` should preferably link to an issue; if none exists, link the PR.
- New scaler template: `**General**: Introduce new XXXXXX Scaler ([#ISSUE](https://github.com/kedacore/keda/issues/ISSUE))`.
- Internal-only changes (refactors, test-only changes, CI tweaks) do **not** require a changelog entry.

## Including Documentation Changes

For any contribution you make that impacts the behavior or experience of KEDA, please open a corresponding docs request for [keda.sh](https://keda.sh) through [https://github.com/kedacore/keda-docs](https://github.com/kedacore/keda-docs).  Contributions that do not include documentation or samples will be rejected.

Manifest changes that affect deployment also require a matching PR against [`kedacore/charts`](https://github.com/kedacore/charts).

Link both docs and charts PRs from the `Relates to` line in the PR template.

## Creating and building a local environment

[Details on setup of a development environment are found on the README](./BUILD.md)

## Developer Certificate of Origin: Signing your work

### Every commit needs to be signed

The Developer Certificate of Origin (DCO) is a lightweight way for contributors to certify that they wrote or otherwise have the right to submit the code they are contributing to the project. Here is the full text of the [DCO](https://developercertificate.org)

Contributors sign-off that they adhere to these requirements by adding a `Signed-off-by` line to commit messages.

```
This is my commit message

Signed-off-by: Random J Developer <random@developer.example.org>
```
Git even has a `-s` command line option to append this automatically to your commit message:
```
$ git commit -s -m 'This is my commit message'
```

Each Pull Request is checked  whether or not commits in a Pull Request do contain a valid Signed-off-by line.

### Commit hygiene

- **Every commit must be signed off** (DCO). The `Signed-off-by:` trailer must match the author. CI rejects PRs with unsigned commits.
- Do not use `--no-verify`, `--no-gpg-sign`, or otherwise skip hooks unless a maintainer has explicitly asked you to.
- Do not commit generated files that are not produced by the documented `make` targets in [Required local checks before opening a PR](#required-local-checks-before-opening-a-pr).
- Do not commit secrets, credentials, `.env` files, or large binaries.

### I didn't sign my commit, now what?!

No worries - You can easily replay your changes, sign them and force push them!

```
git checkout <branch-name>
git reset $(git merge-base main <branch-name>)
git add -A
git commit -sm "one commit on <branch-name>"
git push --force
```

## Code Quality

This project is using [pre-commits](https://pre-commit.com) to ensure the quality of the code.
We encourage you to use pre-commits, but it's not a required to contribute. Every change is checked
on CI and if it does not pass the tests it cannot be accepted. If you want to check locally then
you should install Python3.6 or newer together and run:
```bash
pip install pre-commit
# or
brew install pre-commit
```
For more installation options visit the [pre-commits](https://pre-commit.com).

Before running pre-commit, you must install the [golangci-lint](https://golangci-lint.run/) tool as a static check tool for golang code (contains a series of linter)
```shell script
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.11.4
# or
brew install golangci/tap/golangci-lint
```
For more installation options visit the [golangci-lint](https://golangci-lint.run/usage/install/).

To turn on pre-commit checks for commit operations in git, run:
```bash
pre-commit install
```
To run all checks on your staged files, run:
```bash
pre-commit run
```
To run all checks on all files, run:
```bash
pre-commit run --all-files
```

## When in doubt

Stop and ask a maintainer rather than guessing. It is better to leave a `TODO` and surface the question in the PR description than to invent behaviour, fabricate API names, or silence failing checks. The [#KEDA Slack channel](https://kubernetes.slack.com/archives/CKZJ36A5D) is a good place to ask.
