use std::path::{Path, PathBuf};

use crate::format::FileNumber;


#[derive(Debug, Clone, Copy)]
pub(crate) enum LevelDBFileName {
    Log {
        file_number: FileNumber,
    },
    Lockfile,
    Table {
        file_number: FileNumber,
    },
    TableLegacyExtension {
        file_number: FileNumber,
    },
    Manifest {
        file_number: FileNumber,
    },
    Current,
    Temp {
        file_number: FileNumber,
    },
    InfoLog,
    OldInfoLog,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl LevelDBFileName {
    #[must_use]
    pub fn parse(file_name: &Path) -> Option<Self> {
        // Currently, all valid file names for LevelDB files are valid 7-bit ASCII and thus
        // valid UTF-8.
        let file_name = file_name.to_str()?;

        // Note that all the valid file names are nonempty
        let &first_byte = file_name.as_bytes().first()?;
        // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
        if first_byte == b'+' {
            return None;
        }

        if let Some(file_number) = file_name.strip_suffix(".ldb") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Table { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".log") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Log { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".sst") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::TableLegacyExtension { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".dbtmp") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Temp { file_number })

        } else if let Some(file_number) = file_name.strip_prefix("MANIFEST-") {
            // Any file number, even 0, would make it nonempty.
            let &first_num_byte = file_number.as_bytes().first()?;
            // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
            if first_num_byte == b'+' {
                return None;
            }

            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Manifest { file_number })

        } else {
            Some(match file_name {
                "LOCK"    => Self::Lockfile,
                "CURRENT" => Self::Current,
                "LOG"     => Self::InfoLog,
                "LOG.old" => Self::OldInfoLog,
                _         => return None,
            })
        }
    }

    #[must_use]
    pub fn file_name(self) -> PathBuf {
        match self {
            Self::Log { file_number }      => format!("{:06}.log", file_number.0).into(),
            Self::Lockfile                 => Path::new("LOCK").to_owned(),
            Self::Table { file_number }    => format!("{:06}.ldb", file_number.0).into(),
            Self::TableLegacyExtension { file_number } => format!("{:06}.sst", file_number.0).into(),
            Self::Manifest { file_number } => format!("MANIFEST-{:06}", file_number.0).into(),
            Self::Current                  => Path::new("CURRENT").to_owned(),
            Self::Temp { file_number }     => format!("{:06}.dbtmp", file_number.0).into(),
            Self::InfoLog                  => Path::new("LOG").to_owned(),
            Self::OldInfoLog               => Path::new("LOG.old").to_owned(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;


    /// Tests that the filenames do not have directory components.
    #[test]
    fn file_name_has_no_slash() {
        for file_number in 0..10 {
            for file_name in [
                LevelDBFileName::Log { file_number },
                LevelDBFileName::Table { file_number },
                LevelDBFileName::TableLegacyExtension { file_number },
                LevelDBFileName::Manifest { file_number },
                LevelDBFileName::Temp { file_number },
            ].map(LevelDBFileName::file_name) {
                assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
            }
        }

        for file_name in [
            LevelDBFileName::Lockfile,
            LevelDBFileName::Current,
            LevelDBFileName::InfoLog,
            LevelDBFileName::OldInfoLog,
        ].map(LevelDBFileName::file_name) {
            assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
        }
    }
}
