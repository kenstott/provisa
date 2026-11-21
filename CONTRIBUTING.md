# Contributing

To work on Provisa, clone `main`. To run Provisa, install a release from the [Releases page](https://github.com/kenstott/provisa/releases).

A feature or fix starts from `main` and lands on `main`. Before it lands it needs:

- Unit tests passing
- UI unit tests passing, when it touches the UI
- The tests written for the change
- The smoke check

Releases are cut from `main`, and today every release is an alpha marked latest. [docs/release-process.md](docs/release-process.md) describes the whole process.

The README covers the dev container and how to run the unit suites.
