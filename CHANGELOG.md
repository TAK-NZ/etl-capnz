# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v1.4.0

- :tada: Add `capabilities.json` manifest (validated against `@tak-ps/etl`'s `StaticCapabilitiesSchema`) describing this task's permissions, compute requirements and invocation modes, embedded in the pushed image as the `com.cloudtak.capabilities` OCI annotation via `docker buildx` in CI
- :white_check_mark: Add basic test suite (`npm test`) covering the task's static config and Input/Output schemas
- :rocket: Switch to `Task.init()` for local-dev `ETL_TOKEN` auto-generation (no behavior change in Lambda)
- :arrow_up: Bump `engines` Node version requirement to 24, matching the Dockerfile runtime and `@tak-ps/etl`'s own requirement (CI's existing Node 22 is an intentional, noted divergence - not addressed here)
- :arrow_up: Bump `@tak-ps/etl` 10.9.0 → 10.22.1 (needed for `StaticCapabilitiesSchema`) and update `eslint`, `fast-xml-parser`, `typescript-eslint` to latest within existing semver ranges; resolve all npm audit advisories. `typescript` stays pinned to `^6.0.3` until `typescript-eslint` supports 7.x (its peer dependency currently caps at `<6.1.0`)

### v1.3.8

- :bug: Fix CAP timestamps mislabeled as UTC - `sent`/`onset`/`expires` arrive as local NZ time with a numeric offset, not UTC; convert via `new Date(...).toISOString()` before assigning to `sentUTC`/`onsetUTC`/`expiresUTC`
- :rocket: Group `remarks` timestamp lines by NZ local first, then UTC, instead of alternating UTC/NZ pairs per timestamp

### v1.3.5 - v1.3.7

- :tada: Normalize date/time fields to NZ local + UTC pairs (`sentUTC`/`sentLocal`, `onsetUTC`/`onsetLocal`, `expiresUTC`/`expiresLocal`) in `metadata` and `remarks`, using `Intl.DateTimeFormat` with `Pacific/Auckland` for DST-aware NZST/NZDT formatting and relative time
- :arrow_up: Update dependencies (`eslint`, `typescript-eslint`, `@sinclair/typebox`, `@tak-ps/etl`, `fast-xml-parser`) and resolve npm audit advisories

### v1.0.0

- :tada: Initial Commit
