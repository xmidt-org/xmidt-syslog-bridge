Contribution Guidelines
=======================

We love to see contributions to the project and have tried to make it easy to 
do so. If you would like to contribute code to this project you can do so 
through GitHub by [forking the repository and sending a pull request](http://gun.io/blog/how-to-github-fork-branch-and-pull-request/).

Before Comcast merges your code into the project you must sign the 
[Comcast Contributor License Agreement (CLA)](https://gist.github.com/ComcastOSS/a7b8933dd8e368535378cda25c92d19a).

If you haven't previously signed a Comcast CLA, you'll automatically be asked 
to when you open a pull request. Alternatively, we can e-mail you a PDF that 
you can sign and scan back to us. Please send us an e-mail or create a new 
GitHub issue to request a PDF version of the CLA.

If you have a trivial fix or improvement, please create a pull request and 
request a review from a [maintainer](MAINTAINERS.md) of this repository.

If you plan to do something more involved, that involves a new feature or 
changing functionality, please first create an [issue](#issues) so a discussion of 
your idea can happen, avoiding unnecessary work and clarifying implementation.

A relevant coding style guideline is the [Go Code Review Comments](https://code.google.com/p/go-wiki/wiki/CodeReviewComments).

Documentation
-------------

If you contribute anything that changes the behavior of the application, 
document it in the follow places as applicable:
* the code itself, through clear comments and unit tests
* [README](README.md)

This includes new features, additional variants of behavior, and breaking 
changes.

Testing
-------

Tests are written using golang's standard testing tools, and are run prior to 
the PR being accepted.

The tests are split by whether they need a container. Anything that does sits
behind the `integration` build tag in a `*_integration_test.go` file, so the
unit suite stays fast enough to run on every save.

**The Makefile is the interface. A bare `go test ./...` runs only half the
suite and says nothing about the half it skipped.**

```
make test              unit suite, no containers, under two seconds
make test-integration  integration suite, needs a container runtime
make test-all          both
make vet               go vet over both build configurations
make lint              golangci-lint over both build configurations
make help              the rest
```

`make test-integration` also supplies the container environment, which is the
other reason the Makefile exists. It picks the rootless podman socket when one
is there and falls back to Docker, so the same target works on a developer's
machine and on a CI runner. It turns testcontainers' reaper off, because on an
SELinux host (Fedora, RHEL) the reaper mounts the container socket and SELinux
does not let the `container_t` domain `connectto` the daemon — so it is denied
whatever uid it runs as. Nothing leaks from that: the fixture terminates its own
container in `TestMain`. To keep the reaper instead, run it unconfined:

```
TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true make test-integration
```

A new helper that needs no container belongs on the untagged side, beside its
test, even when its only caller is tagged — otherwise it leaves the fast suite
for no reason.

**Confirm the container tests actually ran.** Within the tagged build,
`requireDocker(t)` skips when no runtime is reachable, and a skipped test still
exits 0 — so a green run can mean nothing was tested. Read the output for
`docker unavailable:` before trusting it. To see the containers for yourself:

```
go test -tags=integration -v -count=1 -run TestKafkaFixture .
```

which prints the image, the container id, and per-test timings. The crude check
is the clock: the integration suite takes around thirty seconds, so anything
faster did not start a broker.

CI runs the two suites in separate workflows: the shared-go `ci.yml` runs the
unit suite, and `integration.yml` runs `make test-integration` on a runner that
has Docker.

Issues
------

For creating an issue:
* **Bugs:** please be as thorough as possible, with steps to recreate the issue 
  and any other relevant information.
* **Feature Requests:** please include functionality and use cases.  If this is 
  an extension of a current feature, please include whether or not this would 
  be a breaking change or how to extend the feature with backwards 
  compatibility.
* **Security Vulnerability:** please report it at 
  https://my.xfinity.com/vulnerabilityreport and contact the [maintainers](MAINTAINERS.md).

If you wish to work on an issue, please assign it to yourself.  If you have any
questions regarding implementation, feel free to ask clarifying questions on 
the issue itself.

Pull Requests
-------------

* should be narrowly focused with no more than 3 or 4 logical commits
* when possible, address no more than one issue
* should be reviewable in the GitHub code review tool
* should be linked to any issues it relates to (i.e. issue number after (#) in commit messages or pull request message)
* should conform to idiomatic golang code formatting
