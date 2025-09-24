use crate::core::status::Status;
use std::fs::{File as StdFile, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug, Clone, Copy)]
pub enum FileCreateDisposition {
    CreateOrTruncate,
    OpenOrCreate,
    OpenExisting,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FileOptions {
    pub delete_on_close: bool,
}

pub struct File {
    file: Option<StdFile>,
    path: String,
    delete_on_close: bool,
}

impl File {
    pub fn new(path: &str) -> Self {
        Self {
            file: None,
            path: path.to_string(),
            delete_on_close: false,
        }
    }

    pub fn open(
        &mut self,
        disposition: FileCreateDisposition,
        options: FileOptions,
    ) -> Result<(), Status> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true);

        match disposition {
            FileCreateDisposition::CreateOrTruncate => {
                open_options.create(true).truncate(true);
            }
            FileCreateDisposition::OpenOrCreate => {
                open_options.create(true);
            }
            FileCreateDisposition::OpenExisting => {}
        }

        match open_options.open(&self.path) {
            Ok(f) => {
                self.file = Some(f);
                self.delete_on_close = options.delete_on_close;
                Ok(())
            }
            Err(_) => Err(Status::IoError),
        }
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), Status> {
        if let Some(file) = self.file.as_mut() {
            if file.seek(SeekFrom::Start(offset)).is_err() {
                return Err(Status::IoError);
            }
            if file.write_all(data).is_err() {
                return Err(Status::IoError);
            }
            Ok(())
        } else {
            Err(Status::IoError)
        }
    }

    pub fn read(&mut self, offset: u64, data: &mut [u8]) -> Result<(), Status> {
        if let Some(file) = self.file.as_mut() {
            if file.seek(SeekFrom::Start(offset)).is_err() {
                return Err(Status::IoError);
            }
            if file.read_exact(data).is_err() {
                return Err(Status::IoError);
            }
            Ok(())
        } else {
            Err(Status::IoError)
        }
    }

    pub fn close(&mut self) -> Result<(), Status> {
        if self.file.take().is_some()
            && self.delete_on_close
            && std::fs::remove_file(&self.path).is_err()
        {
            return Err(Status::IoError);
        }
        Ok(())
    }
}

impl Clone for File {
    fn clone(&self) -> Self {
        Self {
            file: None, // Cannot clone file handles, will need to reopen
            path: self.path.clone(),
            delete_on_close: self.delete_on_close,
        }
    }
}

impl Drop for File {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
