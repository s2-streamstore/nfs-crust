use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteReadMode {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Direct,
    FadviseDontNeed,
}

impl RemoteReadMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Direct => "o-direct",
            Self::FadviseDontNeed => "posix-fadvise-dontneed-best-effort",
        }
    }
}

pub fn probe_remote_read_mode(root: &Path) -> (RemoteReadMode, String) {
    let probe_dir = root.join("direct-io-probe");
    if let Err(error) = fs::create_dir_all(&probe_dir) {
        return (
            RemoteReadMode::FadviseDontNeed,
            format!("could not create probe directory: {error}"),
        );
    }
    let probe_path = probe_dir.join("block.bin");
    let payload = vec![0x5a; 4096];
    if let Err(error) = write_seed(&probe_path, &payload) {
        return (
            RemoteReadMode::FadviseDontNeed,
            format!("could not write direct-I/O probe: {error}"),
        );
    }

    #[cfg(target_os = "linux")]
    let outcome = match read_direct(&probe_path, payload.len(), 0) {
        Ok(body) if body == payload => (
            RemoteReadMode::Direct,
            "EFS accepted an aligned 4 KiB O_DIRECT read".to_owned(),
        ),
        Ok(_) => (
            RemoteReadMode::FadviseDontNeed,
            "O_DIRECT returned unexpected probe content".to_owned(),
        ),
        Err(error) => (
            RemoteReadMode::FadviseDontNeed,
            format!("O_DIRECT probe failed: {error}"),
        ),
    };

    #[cfg(not(target_os = "linux"))]
    let outcome = (
        RemoteReadMode::FadviseDontNeed,
        "O_DIRECT probe is only implemented on Linux".to_owned(),
    );

    let _ = fs::remove_dir_all(probe_dir);
    outcome
}

pub fn write_seed(path: &Path, data: &[u8]) -> io::Result<()> {
    let parent = parent(path)?;
    fs::create_dir_all(parent)?;
    let mut file = File::create(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    advise_dont_need(&file, data.len() as u64);
    Ok(())
}

pub fn read_remote(
    path: &Path,
    expected_size: usize,
    known_size: bool,
    mode: RemoteReadMode,
) -> io::Result<Vec<u8>> {
    match mode {
        RemoteReadMode::Direct => {
            let size = if known_size {
                let observed = fs::metadata(path)?.len();
                if observed != expected_size as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("file size {observed} did not match supplied size {expected_size}"),
                    ));
                }
                expected_size
            } else {
                usize::try_from(fs::metadata(path)?.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "file size exceeds usize")
                })?
            };
            #[cfg(target_os = "linux")]
            {
                read_direct(path, size, 0)
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = size;
                read_fadvise(path, expected_size, known_size)
            }
        }
        RemoteReadMode::FadviseDontNeed => read_fadvise(path, expected_size, known_size),
    }
}

pub fn read_remote_range(
    path: &Path,
    offset: u64,
    length: usize,
    mode: RemoteReadMode,
) -> io::Result<Vec<u8>> {
    match mode {
        RemoteReadMode::Direct => {
            #[cfg(target_os = "linux")]
            {
                read_direct(path, length, offset)
            }
            #[cfg(not(target_os = "linux"))]
            {
                read_fadvise_range(path, offset, length)
            }
        }
        RemoteReadMode::FadviseDontNeed => read_fadvise_range(path, offset, length),
    }
}

pub fn read_remote_trusted_size(
    path: &Path,
    expected_size: usize,
    mode: RemoteReadMode,
) -> io::Result<Vec<u8>> {
    match mode {
        RemoteReadMode::Direct => {
            #[cfg(target_os = "linux")]
            {
                read_direct(path, expected_size, 0)
            }
            #[cfg(not(target_os = "linux"))]
            {
                read_trusted_size(path, expected_size)
            }
        }
        RemoteReadMode::FadviseDontNeed => read_trusted_size(path, expected_size),
    }
}

pub fn read_cached(path: &Path, expected_size: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut body = Vec::with_capacity(expected_size);
    file.read_to_end(&mut body)?;
    Ok(body)
}

pub fn warm_page_cache(path: &Path, expected_size: usize) -> io::Result<()> {
    let body = read_cached(path, expected_size)?;
    if body.len() != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected {expected_size} bytes, read {}", body.len()),
        ));
    }
    Ok(())
}

pub fn atomic_put(
    destination: &Path,
    data: &[u8],
    create_new: bool,
    temporary_suffix: &str,
) -> io::Result<()> {
    let parent = parent(destination)?;
    let temporary = temporary_path(parent, temporary_suffix);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(&temporary)?;

    let prepared = (|| {
        file.write_all(data)?;
        file.sync_all()?;
        let observed_size = file.metadata()?.len();
        if observed_size != data.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "prepared file size {observed_size} did not match {}",
                    data.len()
                ),
            ));
        }
        drop(file);
        if create_new {
            fs::hard_link(&temporary, destination)?;
            // Publication is complete once LINK succeeds. A failed temporary
            // cleanup must not turn a successful create-new into a false
            // operation failure; scenario cleanup removes any remnant.
            let _ = fs::remove_file(&temporary);
        } else {
            fs::rename(&temporary, destination)?;
        }
        Ok(())
    })();

    if prepared.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    prepared
}

pub fn metadata_size(path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub fn delete(path: &Path) -> io::Result<()> {
    fs::remove_file(path)
}

pub fn list_count(path: &Path) -> io::Result<usize> {
    fs::read_dir(path)?.try_fold(0_usize, |count, entry| {
        entry.map(|_| count.saturating_add(1))
    })
}

fn read_fadvise(path: &Path, expected_size: usize, known_size: bool) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let size = if known_size {
        expected_size
    } else {
        usize::try_from(file.metadata()?.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file size exceeds usize"))?
    };
    advise_dont_need(&file, size as u64);
    let mut body = Vec::with_capacity(size);
    file.read_to_end(&mut body)?;
    advise_dont_need(&file, size as u64);
    Ok(body)
}

fn read_trusted_size(path: &Path, expected_size: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    advise_dont_need(&file, expected_size as u64);
    let mut body = Vec::with_capacity(expected_size);
    file.take(expected_size as u64).read_to_end(&mut body)?;
    if body.len() != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected {expected_size} bytes, read {}", body.len()),
        ));
    }
    Ok(body)
}

fn read_fadvise_range(path: &Path, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    advise_dont_need(&file, file.metadata()?.len());
    file.seek(SeekFrom::Start(offset))?;
    let mut body = vec![0_u8; length];
    file.read_exact(&mut body)?;
    advise_dont_need(&file, file.metadata()?.len());
    Ok(body)
}

#[cfg(target_os = "linux")]
fn read_direct(path: &Path, size: usize, offset: u64) -> io::Result<Vec<u8>> {
    const ALIGNMENT: usize = 4096;
    if size == 0 || !size.is_multiple_of(ALIGNMENT) || !offset.is_multiple_of(ALIGNMENT as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "O_DIRECT reads require non-zero 4 KiB-aligned length and offset",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc::O_DIRECT);
    let file = options.open(path)?;
    let buffer = AlignedBuffer::new(size, ALIGNMENT)?;
    let mut total = 0_usize;
    while total < size {
        // SAFETY: the remaining buffer is allocated and aligned whenever a
        // continuation is attempted, the fd is readable, and the offset is
        // block-aligned.
        let read = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buffer.as_mut_ptr().add(total).cast(),
                size - total,
                (offset + total as u64) as libc::off_t,
            )
        };
        if read < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as usize);
        if total < size && !total.is_multiple_of(ALIGNMENT) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("O_DIRECT returned an unaligned short read of {total} bytes"),
            ));
        }
    }
    if total != size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("O_DIRECT expected {size} bytes, read {total}"),
        ));
    }
    // SAFETY: pread initialized exactly `size` bytes after the check above.
    Ok(unsafe { std::slice::from_raw_parts(buffer.as_mut_ptr(), size) }.to_vec())
}

#[cfg(target_os = "linux")]
struct AlignedBuffer {
    pointer: std::ptr::NonNull<u8>,
}

#[cfg(target_os = "linux")]
impl AlignedBuffer {
    fn new(size: usize, alignment: usize) -> io::Result<Self> {
        let mut pointer = std::ptr::null_mut();
        // SAFETY: posix_memalign writes one allocation pointer on success.
        let result = unsafe { libc::posix_memalign(&mut pointer, alignment, size) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result));
        }
        let pointer = std::ptr::NonNull::new(pointer.cast())
            .ok_or_else(|| io::Error::other("posix_memalign returned null"))?;
        Ok(Self { pointer })
    }

    fn as_mut_ptr(&self) -> *mut u8 {
        self.pointer.as_ptr()
    }
}

#[cfg(target_os = "linux")]
impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: the pointer came from posix_memalign and has not been freed.
        unsafe { libc::free(self.pointer.as_ptr().cast()) };
    }
}

fn advise_dont_need(file: &File, length: u64) {
    #[cfg(target_os = "linux")]
    {
        // This is advisory. Failure is intentionally non-fatal and the exact
        // policy is recorded in the report as best effort when O_DIRECT is not
        // available.
        unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                0,
                length.min(i64::MAX as u64) as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, length);
}

fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))
}

fn temporary_path(parent: &Path, suffix: &str) -> PathBuf {
    parent.join(format!(".nfs-crust-bench-tmp-{suffix}"))
}
