use std::ffi::OsStr;
use std::process::{Command, Stdio};

use anyhow::anyhow;

use crate::package_cache::PackageCacheWriter;
use crate::data::{Channel, Package, Target};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CargoCommand {
    Check,
    Clippy,
    Test,
}

impl CargoCommand {
    pub const fn all_commands() -> &'static [Self] {
        &[Self::Check, Self::Clippy, Self::Test]
    }

    pub const fn default_commands() -> &'static [Self] {
        &[Self::Clippy]
    }

    pub fn parse(command: &str) -> anyhow::Result<Self> {
        Ok(match command {
            "check"  => Self::Check,
            "clippy" => Self::Clippy,
            "test"   => Self::Test,
            _ => return Err(anyhow!("Unknown cargo command name: {command}")),
        })
    }

    /// May panic.
    pub fn run<S: AsRef<OsStr>>(
        self,
        channels:   &[Channel],
        targets:    &[Target],
        packages:   &[Package],
        on_save:    bool,
        extra_args: &[S],
    ) {
        // Assume that `--message-format=json` is enabled if and only if
        // `on_save` is true.
        let msg_fmt_json = on_save;

        if self == Self::Test {
            // `--message-format=json` and targets don't really work for `cargo test`.
            if msg_fmt_json {
                return;
            }

            for &package in packages {
                let mut writer = PackageCacheWriter::new(package, msg_fmt_json);

                for &channel in channels {
                    // The base command for `test`
                    let mut command = self.base_command(channel);

                    // Output to the corresponding cache file
                    command.stderr(Stdio::piped());

                    // Normal flags
                    command.args(package.flags(channel, &Target::Native));

                    command.args(extra_args);

                    let child = command
                        .spawn()
                        .expect("Failed to spawn a cargo command");

                    writer.cache_and_print(child);
                }
            }


        } else {

            for &package in packages {
                let mut writer = PackageCacheWriter::new(package, msg_fmt_json);

                for &channel in channels {
                    for target in targets {
                        // The base command for `check` or `clippy`
                        let mut command = self.base_command(channel);

                        // Output to the corresponding cache file
                        if msg_fmt_json {
                            command.stdout(Stdio::piped());
                        } else {
                            command.stderr(Stdio::piped());
                        }

                        // Normal flags
                        if let Some(target_triple) = target.target_triple() {
                            command.args(["--target", target_triple]);
                        }
                        command.args(package.flags(channel, target));

                        // Make it return stuff that rust-analyzer can parse,
                        // and do less work in --feature-powerset
                        if on_save {
                            command.args(["--message-format=json", "--depth", "1"]);
                        }

                        command.args(extra_args);

                        let child = command
                            .spawn()
                            .expect("Failed to spawn a cargo command");

                        writer.cache_and_print(child);
                    }
                }
            }
        }
    }

    pub fn base_command(self, channel: Channel) -> Command {
        let mut command = Command::new("cargo");
        command.env("RUSTFLAGS", self.rust_flags(channel));
        let channel_arg = match channel {
            Channel::Stable     => "+stable",
            Channel::Nightly    => "+nightly",
            Channel::StableMSRV => "+1.85",
        };
        command.arg(channel_arg);
        match self {
            Self::Check  => command.args(["hack", "check", "--feature-powerset"]),
            Self::Clippy => command.args(["hack", "clippy", "--feature-powerset"]),
            Self::Test   => command.args(["hack", "test", "--feature-powerset"]),
        };
        command.args(["--color", "always"]);
        command
    }

    pub const fn rust_flags(self, channel: Channel) -> &'static str {
        match (self, channel) {
            (_, Channel::Stable | Channel::StableMSRV) => "",
            (Self::Check | Self::Test, Channel::Nightly) => "-Zpolonius",
            (Self::Clippy, Channel::Nightly) => "\
                -Zpolonius \
                -Zcrate-attr=feature(\
                    strict_provenance_lints,\
                    multiple_supertrait_upcastable,\
                    must_not_suspend,\
                    non_exhaustive_omitted_patterns_lint,\
                    supertrait_item_shadowing,\
                    unqualified_local_imports\
                ) \
                -Wfuzzy_provenance_casts \
                -Wlossy_provenance_casts \
                -Wmultiple_supertrait_upcastable \
                -Wmust_not_suspend \
                -Wnon_exhaustive_omitted_patterns \
                -Wsupertrait_item_shadowing_definition \
                -Wsupertrait_item_shadowing_usage \
                -Wunqualified_local_imports",
        }
    }
}
