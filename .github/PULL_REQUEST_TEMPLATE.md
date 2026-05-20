<!-- Thanks for contributing to Hearth! Please fill out this template. -->

## What does this change?

<!-- One-paragraph summary. Why is this change needed, not just what it does. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / cleanup
- [ ] Documentation
- [ ] Configuration / infra
- [ ] Other (describe):

## Linked issues

<!-- Closes #N, refs #M -->

## How was this tested?

<!-- "Ran the stack locally with `docker compose up`, sent a request via the
     gateway, watched the spark-03 GPU ring rise to 42%." Manual testing
     is fine — there's no test suite yet. -->

## Screenshots (if UI change)

<!-- Drag and drop here -->

## Checklist

- [ ] Followed [Conventional Commits](https://www.conventionalcommits.org/) in commit messages
- [ ] Signed off commits (`git commit -s`) — DCO
- [ ] Read [`CONTRIBUTING.md`](../CONTRIBUTING.md)
- [ ] If this changes the data shape, both the SSE handler and the mock simulator in `data.js` write into the same structure
- [ ] If a backend doesn't expose a metric, the UI shows `—` (not a fake `0`)
- [ ] If user-facing strings were added, they're keyed in `i18n.js` for translation
