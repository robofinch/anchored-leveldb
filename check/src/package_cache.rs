use std::{fs, io};
use std::{fs::File, process::Child};
use std::{
    collections::{HashSet, VecDeque},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::data::Package;


const CHECK_CACHE_DIR: &str = ".check-cache";


#[derive(Debug)]
pub struct PackageCacheWriter {
    file:          BufWriter<File>,
    msg_fmt_json:  bool,
    messages_seen: HashSet<String>,
}

impl PackageCacheWriter {
    /// May panic.
    pub fn new(package: Package, msg_fmt_json: bool) -> Self {
        fs::create_dir_all(CHECK_CACHE_DIR)
            .expect("Could not create `CHECK_CACHE_DIR`");

        let package_cache = package_cache_path(package, msg_fmt_json);

        File::create(&package_cache)
            .map(|file| Self {
                file:          BufWriter::new(file),
                msg_fmt_json,
                messages_seen: HashSet::new(),
            })
            .expect("Could not create cache file for a certain package")
    }

    /// May panic.
    pub fn cache_and_print(&mut self, mut child: Child) {
        if self.msg_fmt_json {
            let stdout = child
                .stdout
                .take()
                .expect("When `msg_fmt_json`, the child's stdout should be piped");

            let reader = BufReader::new(stdout);

            for line in reader.lines() {
                let line = line.unwrap();

                if line.starts_with(r#"{"reason":"compiler-message""#) {
                    if !self.messages_seen.contains(&line) {
                        self.file.write_all(line.as_bytes()).unwrap();
                        self.file.write_all(b"\n").unwrap();
                        println!("{line}");
                        self.messages_seen.insert(line);
                    }
                } else {
                    self.file.write_all(line.as_bytes()).unwrap();
                    self.file.write_all(b"\n").unwrap();
                    println!("{line}");
                }
            }
        } else {
            let stderr = child
                .stderr
                .take()
                .expect("When `!msg_fmt_json`, the child's stderr should be piped");

            let reader = BufReader::new(stderr);

            for line in reader.lines() {
                let line = line.unwrap();

                self.file.write_all(line.as_bytes()).unwrap();
                self.file.write_all(b"\n").unwrap();
                eprintln!("{line}");
            }
        }

        let exit_status = child
            .wait()
            .expect("Waiting on a cargo command failed");

        if !exit_status.success() {
            panic!("A cargo command exited with unsuccesful status {exit_status}");
        }
    }
}

/// Determine which packages need to be rechecked.
///
/// May panic.
///
/// Logic:
///
/// ```no_run
/// # let no_cache = false;
/// if no_cache {
///     // (NO output for anything else)
///     // check every package in args.packages
/// } else {
///     // (output cached messages for the rest of args.packages)
///     // check any package in args.packages whose cache is too old or doesn't exist
/// }
/// ```
pub fn packages_to_check(
    args_packages:    &[Package],
    on_save:          bool,
    no_cache:         bool,
) -> Vec<Package> {
    // Assume that `--message-format=json` is enabled if and only if
    // `on_save` is true.
    let msg_fmt_json = on_save;

    let mut to_check = Vec::new();

    if no_cache {
        to_check.extend(args_packages);

    } else {
        for &package in Package::all_packages() {
            if args_packages.contains(&package)
                && is_package_cache_invalid(package, msg_fmt_json)
            {
                to_check.push(package);
            }
        }
    }

    to_check
}

/// Print any cached messages that should be printed.
///
/// May panic.
///
/// Logic:
///
/// ```no_run
/// # let no_cache = false;
/// if no_cache {
///     // NO output for anything else
///     // (check every package in args.packages)
/// } else {
///     // output cached messages for the rest of args.packages
///     // (check any package in args.packages whose cache is too old or doesn't exist)
/// }
/// ```
pub fn print_cached_checks(
    args_packages:    &[Package],
    checked_packages: &[Package],
    on_save:          bool,
    no_cache:         bool,
) {
    // Assume that `--message-format=json` is enabled if and only if
    // `on_save` is true.
    let msg_fmt_json = on_save;

    let packages_to_print = if no_cache {
        Vec::new()
    } else {
        args_packages
            .iter()
            .filter(|package| !checked_packages.contains(package))
            .collect()
    };

    macro_rules! inner_print {
        ($writer:expr) => {
            for &package in packages_to_print {

                if !fs::exists(package_cache_path(package, msg_fmt_json)).unwrap() {
                    continue;
                }

                // Read from the package check output in the cache, write to the writer
                io::copy(
                    &mut read_cache_for_package(package, msg_fmt_json),
                    $writer,
                ).unwrap();
            }
        };
    }

    if msg_fmt_json {
        inner_print!(&mut io::stdout().lock());
    } else {
        inner_print!(&mut io::stderr().lock());
    }
}

/// Get the path to the cache file for the given package and format.
pub fn package_cache_path(package: Package, msg_fmt_json: bool) -> PathBuf {
    let mut cache_filename = package.package_name().to_owned();

    if msg_fmt_json {
        cache_filename.push_str(".msg-fmt-json");
    } else {
        cache_filename.push_str(".ansi");
    }

    Path::new(CHECK_CACHE_DIR).join(cache_filename)
}

/// Check whether the package's cache either does not exist, or was invalidated due to a
/// dependency changing.
///
/// Note that this DOES NOT take into account the fact that a different
/// command might have been run on the same package and format.
///
/// May panic.
pub fn is_package_cache_invalid(package: Package, msg_fmt_json: bool) -> bool {
    let package_cache = package_cache_path(package, msg_fmt_json);

    match fs::metadata(package_cache) {
        Ok(meta) => {
            let modified = meta
                .modified()
                .expect("could not check the modified time of a package cache");

            let mut dependencies = VecDeque::from(package.dependencies());

            while let Some(dependency) = dependencies.pop_front() {
                let metadata = fs::metadata(&dependency).unwrap();

                if metadata.modified().unwrap() > modified {
                    return true;
                } else if metadata.is_dir() {
                    for dir_entry in fs::read_dir(&dependency).unwrap() {
                        let entry_name = dir_entry.unwrap().file_name();
                        let entry_path = dependency.join(&entry_name);

                        if entry_name == "target" && fs::metadata(&entry_path).unwrap().is_dir() {
                            // Skip `target` directories.
                            continue;
                        }

                        dependencies.push_back(entry_path);
                    }
                }
            }

            false
        }
        Err(_) => true,
    }
}

/// Get the file which stores cached messages (of the indicated format) for the given package.
///
/// May panic.
pub fn read_cache_for_package(package: Package, msg_fmt_json: bool) -> File {
    let package_cache = package_cache_path(package, msg_fmt_json);

    File::open(&package_cache).unwrap()
}
