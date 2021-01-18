# ddlog_benches

DDlog has a unique benchmarking scenario since it's a compiler, so benchmarking the programs
it generates is a little more involved than normal, which is why we use [`cargo-make`] to automate
the setup, build and bench process in a cross-platform way. To run the benchmarks in this crate,
first install `cargo-make` with the following command (`--force` makes cargo install the latest version)

```sh
cargo install --force cargo-make
```

[`cargo-make`]: https://github.com/sagiegurari/cargo-make
