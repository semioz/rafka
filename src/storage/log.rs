use std::path::{PathBuf};
use std::io::{self, Seek, SeekFrom, Write, Read};
use std::fs::{File, OpenOptions, create_dir_all};
use fs2::FileExt;

// entire commit log for a single partition
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    active_segment: LogSegment,
    segments: Vec<LogSegment>,
    max_segment_size: u64,
}

// single file on disk storing a contiguous block of messages
#[derive(Debug)]
pub struct LogSegment {
    base_offset: i64,
    file: File,
    path: PathBuf,
    position: u64,
}

impl LogSegment {
    pub fn new(base_offset: i64, path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        file.lock_exclusive()?;

        let position = file.metadata()?.len();

        Ok(Self {
            base_offset,
            file,
            path,
            position,
        })
    }

    // framed message to the log segment
    pub fn write_message(&mut self, data: &[u8]) -> io::Result<u64> {
        let pos = self.position;

        // Frame: 4 bytes length prefix (big endian)
        let len = (data.len() as u32).to_be_bytes();

        self.file.write_all(&len)?;
        self.file.write_all(data)?;
        self.position += 4 + data.len() as u64;

        Ok(pos)
    }

    pub fn read_all(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut messages = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = io::BufReader::new(&self.file);

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {
                    let msg_len = u32::from_be_bytes(len_buf) as usize;
                    let mut msg_buf = vec![0u8; msg_len];
                    reader.read_exact(&mut msg_buf)?;
                    messages.push(msg_buf);
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(messages)
    }

    pub fn truncate_before(&mut self, offset: u64) -> io::Result<()> {
        // todo
        Ok(())
    }
}

impl Drop for LogSegment {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl Log {
    pub fn new(dir: PathBuf, base_offset: i64, max_segment_size: u64) -> io::Result<Self> {
        create_dir_all(&dir)?;
        let path = dir.join(format!("{:020}.log", base_offset));
        let active_segment = LogSegment::new(base_offset, path.clone())?;

        Ok(Self {
            dir,
            active_segment,
            segments: vec![],
            max_segment_size,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        if self.active_segment.position + 4 + data.len() as u64 > self.max_segment_size {
            // rotate segment
            let next_offset = self.active_segment.base_offset + 1;
            let new_path = self.dir.join(format!("{:020}.log", next_offset));
            let new_segment = LogSegment::new(next_offset, new_path)?;
            // flush and unlock old segment automatically via drop
            self.segments.push(std::mem::replace(&mut self.active_segment, new_segment));
        }

        let pos = self.active_segment.write_message(data)?;
        // flush once on rotation, not on every message for performance
        if self.active_segment.position == data.len() as u64 + 4 {
            self.active_segment.file.flush()?;
        }

        Ok(pos)
    }

    pub fn read_active_segment(&mut self) -> io::Result<Vec<Vec<u8>>> {
        self.active_segment.read_all()
    }

    pub fn truncate_before(&mut self, offset: u64) -> io::Result<()> {
        while let Some(first) = self.segments.first() {
            if (first.base_offset as u64) < offset {
                let seg = self.segments.remove(0);
                std::fs::remove_file(seg.path)?;
            } else {
                break;
            }
        }

        // truncate active segment if needed (complex, placeholder)
        self.active_segment.truncate_before(offset)?;
        Ok(())
    }
}
