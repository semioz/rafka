/*
use std::path::{Path, PathBuf};
use std::io;
use std::fs::{self, File, OpenOptions};

pub struct Log {
    dir: PathBuf,
    active_segment: LogSegment,
    segments: Vec<LogSegment>,
    max_segment_size: u64,
}

pub struct LogSegment {
    base_offset: i64,
    file: File,
    path: PathBuf,
    position: u64,
}
*/