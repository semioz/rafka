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
    next_offset: i64, // gotta track next logical offset
}

// single file on disk storing a contiguous block of messages
#[derive(Debug)]
pub struct LogSegment {
    base_offset: i64,
    file: File,
    path: PathBuf,
    position: u64,
    message_count: u64, // for tracking messages in this segment
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
            message_count: 0,
        })
    }

    pub fn write_message(&mut self, offset: i64, data: &[u8]) -> io::Result<u64> {
        let pos = self.position;

        let total_len = (8 + data.len() as u32).to_be_bytes();
        let offset_bytes = offset.to_be_bytes();

        self.file.write_all(&total_len)?;
        self.file.write_all(&offset_bytes)?;
        self.file.write_all(data)?;
        self.position += 4 + 8 + data.len() as u64;
        self.message_count += 1;

        Ok(pos)
    }

    pub fn read_all(&mut self) -> io::Result<Vec<(i64, Vec<u8>)>> {
        let mut messages = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = io::BufReader::new(&self.file);

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {
                    let total_len = u32::from_be_bytes(len_buf) as usize;
                    if total_len < 8 {
                        break;
                    }
                    
                    let mut offset_buf = [0u8; 8];
                    reader.read_exact(&mut offset_buf)?;
                    let offset = i64::from_be_bytes(offset_buf);
                    
                    let data_len = total_len - 8;
                    let mut msg_buf = vec![0u8; data_len];
                    reader.read_exact(&mut msg_buf)?;
                    messages.push((offset, msg_buf));
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(messages)
    }

    pub fn truncate_before(&mut self, offset: i64) -> io::Result<()> {
        if offset <= self.base_offset {
            return Ok(());
        }

        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = io::BufReader::new(&self.file);
        let mut truncate_pos = 0u64;

        loop {
            let current_pos = truncate_pos;
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {
                    let total_len = u32::from_be_bytes(len_buf) as u64;
                    if total_len < 8 {
                        break;
                    }
                    
                    let mut offset_buf = [0u8; 8];
                    reader.read_exact(&mut offset_buf)?;
                    let msg_offset = i64::from_be_bytes(offset_buf);
                    
                    if msg_offset >= offset {
                        truncate_pos = current_pos;
                        break;
                    }
                    
                    // Skip message data
                    let data_len = total_len - 8;
                    let mut skip_buf = vec![0u8; data_len as usize];
                    reader.read_exact(&mut skip_buf)?;
                    truncate_pos = current_pos + 4 + total_len;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        // truncate file
        self.file.set_len(truncate_pos)?;
        self.position = truncate_pos;
        Ok(())
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.message_count as i64 - 1
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
            next_offset: base_offset,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<i64> {
        if self.active_segment.position + 4 + 8 + data.len() as u64 > self.max_segment_size {
            // rotate segment - use proper next offset
            let next_base_offset = self.next_offset;
            let new_path = self.dir.join(format!("{:020}.log", next_base_offset));
            let new_segment = LogSegment::new(next_base_offset, new_path)?;
            self.segments.push(std::mem::replace(&mut self.active_segment, new_segment));
        }

        let offset = self.next_offset;
        self.active_segment.write_message(offset, data)?;
        self.next_offset += 1;

        // flush once on rotation, not on every message for performance
        if self.active_segment.message_count == 1 {
            self.active_segment.file.flush()?;
        }

        Ok(offset)
    }

    pub fn read_active_segment(&mut self) -> io::Result<Vec<(i64, Vec<u8>)>> {
        self.active_segment.read_all()
    }

    pub fn read_message(&mut self, offset: i64) -> io::Result<Option<Vec<u8>>> {
        // checking active segment first
        if offset >= self.active_segment.base_offset {
            let messages = self.active_segment.read_all()?;
            for (msg_offset, data) in messages {
                if msg_offset == offset {
                    return Ok(Some(data));
                }
            }
        }

        // check historical segments
        for segment in self.segments.iter_mut().rev() {
            if offset >= segment.base_offset && offset <= segment.last_offset() {
                let messages = segment.read_all()?;
                for (msg_offset, data) in messages {
                    if msg_offset == offset {
                        return Ok(Some(data));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn truncate_before(&mut self, offset: i64) -> io::Result<()> {
        // remove entire segments before offset
        while let Some(first) = self.segments.first() {
            if first.last_offset() < offset {
                let path = first.path.clone();
                let _ = self.segments.remove(0);
                std::fs::remove_file(path)?;
            } else {
                break;
            }
        }

        // Truncate segments that contain the offset
        for segment in &mut self.segments {
            if offset > segment.base_offset && offset <= segment.last_offset() {
                segment.truncate_before(offset)?;
                break;
            }
        }

        // Truncate active segment if needed
        if offset > self.active_segment.base_offset {
            self.active_segment.truncate_before(offset)?;
            // Update next_offset to continue from truncation point
            self.next_offset = offset;
        }

        Ok(())
    }

    pub fn get_latest_offset(&self) -> i64 {
        self.next_offset - 1
    }
}