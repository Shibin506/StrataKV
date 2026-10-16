use std::fmt;
use std::io;
use std::path::PathBuf;

/// Everything that can go wrong. Corruption is never hidden: a damaged record in the middle of a
/// log is an error, not something to skip.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// A file's contents do not match its checksums (or are impossible to decode).
    Corruption {
        file: PathBuf,
        offset: u64,
        reason: String,
    },
    /// Another process (or another `Db` in this process) has the database open.
    Locked(PathBuf),
    InvalidArgument(String),
    /// An earlier write failed in a way that leaves the log's state unknown (for example a failed
    /// fsync). Every later write is refused until the database is reopened, which re-reads the log.
    Poisoned(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption { file, offset, reason } => {
                write!(f, "corruption in {} at byte {offset}: {reason}", file.display())
            }
            Error::Locked(p) => write!(f, "database {} is already open (LOCK is held)", p.display()),
            Error::InvalidArgument(s) => write!(f, "invalid argument: {s}"),
            Error::Poisoned(s) => write!(f, "database refuses writes after an earlier failure: {s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
