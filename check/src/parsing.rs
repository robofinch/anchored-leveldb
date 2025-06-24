use std::env;
use std::collections::HashSet;

use anyhow::anyhow;

use crate::{commands::CargoCommand, data::{Channel, Package, Target}};


#[derive(Debug)]
pub struct ParsedArgs {
    pub commands:         Vec<CargoCommand>,
    pub channels:         Vec<Channel>,
    pub targets:          Vec<Target>,
    pub packages:         Vec<Package>,
    pub on_save:          bool,
    pub no_cache:         bool,
    pub trailing_args:    Vec<String>,
}

impl ParsedArgs {
    pub fn try_parse() -> anyhow::Result<Self> {
        let raw_args = RawArgs::try_parse()?;

        macro_rules! args_field_vec {
            ($set_field:ident, $all:ident, $default:ident, $field_type:ident) => {
                if raw_args.$all {
                    $field_type::$all().to_vec()
                } else if raw_args.$set_field.is_empty() {
                    $field_type::$default().to_vec()
                } else {
                    $field_type::$all()
                        .iter()
                        .cloned()
                        .filter(|thing| raw_args.$set_field.contains(thing))
                        .collect()
                }
            };
        }

        Ok(Self {
            commands: args_field_vec!(commands, all_commands, default_commands, CargoCommand),
            channels: args_field_vec!(channels, all_channels, default_channels, Channel),
            targets:  args_field_vec!(targets,  all_targets,  default_targets,  Target),
            packages: args_field_vec!(packages, all_packages, default_packages, Package),
            on_save:          raw_args.on_save,
            no_cache:         raw_args.no_cache,
            trailing_args:    raw_args.trailing_args,
        })
    }
}

#[derive(Default, Debug)]
struct RawArgs {
    commands:         HashSet<CargoCommand>,
    channels:         HashSet<Channel>,
    targets:          HashSet<Target>,
    packages:         HashSet<Package>,
    on_save:          bool,
    all_commands:     bool,
    all_channels:     bool,
    all_targets:      bool,
    all_packages:     bool,
    no_cache:         bool,
    trailing_args:    Vec<String>,
}

impl RawArgs {
    fn try_parse() -> anyhow::Result<Self> {
        let mut input_args = env::args().skip(1);
        let mut raw_args = Self::default();

        while let Some(input_arg) = input_args.next() {
            match &*input_arg {
                "--" => {
                    raw_args.trailing_args.extend(input_args);
                    break;
                }
                "--command" => {
                    let next_arg = input_args
                        .next()
                        .ok_or_else(|| anyhow!("Missing argument after `--command`"))?;

                    raw_args.commands.insert(CargoCommand::parse(&next_arg)?);
                }
                "--channel" => {
                    let next_arg = input_args
                        .next()
                        .ok_or_else(|| anyhow!("Missing argument after `--channel`"))?;

                    raw_args.channels.insert(Channel::parse(&next_arg)?);
                }
                "--target" => {
                    let next_arg = input_args
                        .next()
                        .ok_or_else(|| anyhow!("Missing argument after `--target`"))?;

                    raw_args.targets.insert(Target::parse(next_arg));
                }
                "--package" => {
                    let next_arg = input_args
                        .next()
                        .ok_or_else(|| anyhow!("Missing argument after `--package`"))?;

                    raw_args.packages.insert(Package::parse(&next_arg)?);
                }
                "--all" => {
                    raw_args.all_commands = true;
                    raw_args.all_channels = true;
                    raw_args.all_targets  = true;
                    raw_args.all_packages = true;
                }
                "--all-commands"     => raw_args.all_commands = true,
                "--all-channels"     => raw_args.all_channels = true,
                "--all-targets"      => raw_args.all_targets  = true,
                "--all-packages"     => raw_args.all_packages = true,
                "--on-save"          => raw_args.on_save          = true,
                "--no-cache"         => raw_args.no_cache         = true,
                other => {
                    return Err(anyhow!(
                        "Unknown argument: {other} (maybe you meant to pass it after \"--\")",
                    ));
                }
            }
        }

        Ok(raw_args)
    }
}
