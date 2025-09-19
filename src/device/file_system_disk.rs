use crate::core::status::Status;
use crate::environment::file::{File, FileCreateDisposition, FileOptions};
use crate::hlog::persistent_memory_malloc::Disk;

/// An implementation of the `Disk` trait for the local file system.
#[derive(Clone)]
pub struct FileSystemDisk {
    root_path: String,
    log: File,
}

impl FileSystemDisk {
    pub fn new(root_path: &str) -> Result<Self, Status> {
        let path = std::path::Path::new(root_path);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|_| Status::IoError)?;
        }
        let log_path = path.join("hlog.log");
        let mut log = File::new(log_path.to_str().unwrap());
        let status = log.open(FileCreateDisposition::OpenOrCreate, FileOptions::default());
        if status.is_err() {
            return Err(Status::IoError);
        };

        Ok(Self {
            root_path: root_path.to_string(),
            log,
        })
    }

    pub fn log_mut(&mut self) -> &mut File {
        &mut self.log
    }

    pub fn new_file(&self, path: &str) -> File {
        File::new(&format!("{}{}", self.root_path, path))
    }

    pub fn index_checkpoint_path(&self, token: &str) -> String {
        format!("{}/index-checkpoints/{}/", self.root_path, token)
    }

    pub fn create_index_checkpoint_directory(&self, token: &str) -> Result<String, Status> {
        let dir = self.index_checkpoint_path(token);
        if std::path::Path::new(&dir).exists() {
            std::fs::remove_dir_all(&dir).map_err(|_| Status::IoError)?;
        }
        std::fs::create_dir_all(&dir).map_err(|_| Status::IoError)?;
        Ok(dir)
    }

    pub fn log_checkpoint_path(&self, token: &str) -> String {
        format!("{}log-checkpoints/{}/", self.root_path, token)
    }

    pub fn create_log_checkpoint_directory(&self, token: &str) -> Result<String, Status> {
        let dir = self.log_checkpoint_path(token);
        if std::path::Path::new(&dir).exists() {
            std::fs::remove_dir_all(&dir).map_err(|_| Status::IoError)?;
        }
        std::fs::create_dir_all(&dir).map_err(|_| Status::IoError)?;
        Ok(dir)
    }
}

impl Disk for FileSystemDisk {
    fn write_async(
        &mut self,
        offset: u64,
        data: &[u8],
        callback: Box<dyn FnOnce(Status) + Send>,
    ) -> Status {
        // For now, this is a synchronous, blocking write.
        let status = match self.log.write(offset, data) {
            Ok(_) => Status::Ok,
            Err(status) => status,
        };

        // Immediately execute the callback.
        callback(status);
        status
    }

    fn index_checkpoint_path(&self, token: &str) -> String {
        self.index_checkpoint_path(token)
    }
}
