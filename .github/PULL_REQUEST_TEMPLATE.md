## What & why

<!-- One paragraph: what does this PR do, and what's the motivating use case or bug? -->

## How

<!-- Brief: which files changed and why? -->

## Tests

- [ ] All existing tests still pass (`uv run pytest -m "not live"`)
- [ ] New behaviour has a test
- [ ] If this touches the live-API surface, `uv run pytest -m live` is green

## Compatibility

- [ ] No breaking changes to the public MCP tool signatures
- [ ] No breaking changes to the `DataResponse` / `TableDetail` JSON shape
- [ ] CC-BY 4.0 attribution still surfaces in `DataResponse.attribution`
- [ ] If any of the above is broken, this is justified in the PR body

## Other

- [ ] CHANGELOG.md updated
- [ ] README updated if user-visible behaviour changed
- [ ] No new dependencies (or new dep is justified above)
