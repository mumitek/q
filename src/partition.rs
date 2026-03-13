use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path, // Added PathBuf
    sync::Mutex,
};

use tracing::debug;

struct PartitionReader {
    pointer: u64,
    reader: BufReader<File>,
    pointer_writer: File,
}

pub(crate) struct Partition {
    reader: Mutex<PartitionReader>,
    writer: Mutex<File>,
}

// Changed to AsRef<Path> for flexibility
fn read_pointer(pointer_file_name: impl AsRef<Path>) -> Result<(File, u64), Box<dyn Error>> {
    debug!("Reading pointer");
    let path = pointer_file_name.as_ref();
    let pointer = if path.is_file() {
        debug!("Pointer exists");
        let mut reader = File::open(path)?;
        let mut buffer = [0u8; 8];
        reader.read_exact(&mut buffer)?;
        u64::from_be_bytes(buffer)
    } else {
        debug!("Pointer doesn't exist");
        0u64
    };
    let mut writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let pointer_bytes = pointer.to_be_bytes();
    writer.set_len(0)?;
    writer.write_all(&pointer_bytes)?;
    debug!("Written pointer");
    Ok((writer, pointer))
}

impl PartitionReader {
    pub fn try_new(
        file_name: impl AsRef<Path>,
        pointer_file_name: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn Error>> {
        let (pointer_writer, pointer) = read_pointer(pointer_file_name)?;
        let mut reader = File::open(file_name)?;
        reader.seek(SeekFrom::Start(pointer))?;
        let reader = BufReader::new(reader);
        debug!("Created partition reader");
        Ok(Self {
            pointer,
            pointer_writer,
            reader,
        })
    }

    pub fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut message_size_buffer = [0u8; 4];
        self.reader.read_exact(&mut message_size_buffer)?;

        let message_size = u32::from_be_bytes(message_size_buffer);
        let mut message_buffer = vec![0u8; message_size as usize];

        self.reader.read_exact(&mut message_buffer)?;

        self.update_pointer(message_size + message_size_buffer.len() as u32)?;
        Ok(message_buffer)
    }

    fn update_pointer(&mut self, increment: u32) -> Result<(), Box<dyn Error>> {
        self.pointer += increment as u64;
        self.pointer_writer.set_len(0)?;
        self.pointer_writer.seek(SeekFrom::Start(0))?;
        let message_size_buffer = self.pointer.to_be_bytes();
        self.pointer_writer.write_all(&message_size_buffer)?;

        Ok(())
    }
}

impl Partition {
    pub(crate) fn try_new(
        data_file: impl AsRef<Path>,
        pointer_file: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn Error>> {
        debug!("Creating partition writer");
        let writer = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&data_file)?;
        debug!("Creating partition reader");
        let reader = PartitionReader::try_new(data_file, pointer_file)?;
        debug!("Finished creating partition");
        Ok(Partition {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
        })
    }

    pub(crate) fn next(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut reader = self.reader.lock().unwrap();
        reader.read()
    }

    pub(crate) fn add(&self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut writer = self.writer.lock().unwrap();
        let size = (bytes.len() as u32).to_be_bytes();
        writer.write_all(&size)?;
        writer.write_all(bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
    use tempfile::{TempDir, tempdir};

    /// Generates paths in the OS's temporary directory
    fn get_temp_files(id: &str) -> (TempDir, PathBuf, PathBuf) {
        let tmp = tempdir().unwrap();
        let data_path = tmp.path().join(format!("data_{}.bin", id));
        let ptr_path = tmp.path().join(format!("pointer_{}.bin", id));
        dbg!(&data_path, &ptr_path);

        (tmp, data_path, ptr_path)
    }

    #[test]
    fn test_partition_basic_io() {
        let (_keep, data_f, ptr_f) = get_temp_files("basic_io");

        fs::write(&data_f, []).expect("Failed to setup data file");
        fs::write(&ptr_f, vec![0u8; 8]).expect("Failed to setup pointer file");

        let partition =
            Partition::try_new(&data_f, &ptr_f).expect("Failed to initialize partition");

        let msg = b"Hello Rust!";
        partition.add(msg).expect("Failed to write message");

        let read_msg = partition.next().expect("Failed to read message back");

        assert_eq!(
            msg.to_vec(),
            read_msg,
            "Read data does not match written data"
        );
    }

    #[test]
    fn test_pointer_persistence() {
        let (_keep, data_f, ptr_f) = get_temp_files("persistence");

        let msg1 = b"First Message";
        let msg2 = b"Second Message";

        {
            let partition = Partition::try_new(&data_f, &ptr_f).expect("Init session 1 failed");
            partition.add(msg1).expect("Write 1 failed");
            partition.add(msg2).expect("Write 2 failed");

            let res = partition.next().expect("Read 1 failed");
            assert_eq!(res, msg1);
        }

        {
            let partition = Partition::try_new(&data_f, &ptr_f).expect("Init session 2 failed");
            let res = partition
                .next()
                .expect("Read 2 failed - pointer was not persisted");
            assert_eq!(
                res, msg2,
                "Did not resume from the correct pointer position"
            );
        }
    }

    #[test]
    fn test_empty_read_error() {
        let (_keep, data_f, ptr_f) = get_temp_files("empty_err");
        let partition = Partition::try_new(&data_f, &ptr_f).expect("Init failed");

        let result = partition.next();

        assert!(
            result.is_err(),
            "Reading from an empty partition should return an Error"
        );
    }

    #[test]
    fn test_cold_start_creates_files() {
        let (_keep, data_f, ptr_f) = get_temp_files("cold_start");

        let _ = std::fs::remove_file(&data_f);
        let _ = std::fs::remove_file(&ptr_f);

        let partition = Partition::try_new(&data_f, &ptr_f)
            .expect("Failed to initialize partition with non-existent files");

        let msg = b"New Partition Data";
        partition.add(msg).expect("Failed to add first message");

        assert!(data_f.exists(), "Data file was not created");
        assert!(ptr_f.exists(), "Pointer file was not created");

        let result = partition
            .next()
            .expect("Failed to read back the first message");
        assert_eq!(result, msg, "The read data does not match the written data");

        let ptr_content =
            std::fs::read(&ptr_f).expect("Could not read pointer file for verification");
        assert_eq!(ptr_content.len(), 8);

        let current_ptr = u64::from_be_bytes(ptr_content.try_into().unwrap());
        assert_eq!(
            current_ptr,
            4 + msg.len() as u64,
            "Pointer value is incorrect after first write"
        );
    }

    #[test]
    fn test_mixed_push_pop_order() {
        let (_keep, data_f, ptr_f) = get_temp_files("mixed_order");
        let p = Partition::try_new(&data_f, &ptr_f).unwrap();

        // Sequence: Push A, Push B, Pop A, Push C, Pop B, Pop C
        let msg_a = b"Message A";
        let msg_b = b"Message B is longer";
        let msg_c = b"C";

        p.add(msg_a).unwrap();
        p.add(msg_b).unwrap();

        assert_eq!(p.next().unwrap(), msg_a);

        p.add(msg_c).unwrap();

        assert_eq!(p.next().unwrap(), msg_b);
        assert_eq!(p.next().unwrap(), msg_c);
    }

    #[test]
    fn test_edge_case_message_sizes() {
        let (_keep, data_f, ptr_f) = get_temp_files("sizes");
        let p = Partition::try_new(&data_f, &ptr_f).unwrap();

        // 1. Zero-size message
        let empty = b"";
        p.add(empty).unwrap();
        assert_eq!(p.next().unwrap(), empty, "Failed to handle 0-byte message");

        // 2. Large random message (1MB)
        let large_size = 1024 * 1024;
        let large_msg = vec![0u8; large_size]
            .iter()
            .enumerate()
            .map(|(i, _)| (i % 255) as u8)
            .collect::<Vec<_>>();

        p.add(&large_msg).unwrap();
        assert_eq!(p.next().unwrap(), large_msg, "Failed to handle 1MB message");
    }

    #[test]
    fn test_corrupted_pointer_file() {
        let (_keep, data_f, ptr_f) = get_temp_files("corrupt_ptr");

        // Create a pointer file that is too short (e.g., 2 bytes instead of 8)
        fs::write(&ptr_f, vec![0u8; 2]).unwrap();
        fs::write(&data_f, vec![]).unwrap();

        let result = Partition::try_new(&data_f, &ptr_f);

        assert!(
            result.is_err(),
            "Should fail when pointer file is truncated/corrupt"
        );
    }

    #[test]
    fn test_corrupted_data_file() {
        let (_keep, data_f, ptr_f) = get_temp_files("corrupt_data");
        let p = Partition::try_new(&data_f, &ptr_f).unwrap();

        // Add a valid message
        p.add(b"valid").unwrap();

        // Manually append garbage to the data file (a size header that promises 100 bytes, but file ends)
        {
            let mut f = OpenOptions::new().append(true).open(&data_f).unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"too short").unwrap();
        }

        // Read the first valid one
        assert_eq!(p.next().unwrap(), b"valid");

        // The next read should fail because the file ends before the "promised" 100 bytes
        let result = p.next();
        assert!(result.is_err());
    }

    #[test]
    fn test_io_failure_permissions() {
        let (_keep, data_f, ptr_f) = get_temp_files("io_fail");

        // Create files and then make them read-only
        fs::write(&data_f, b"data").unwrap();
        fs::write(&ptr_f, 0u64.to_be_bytes()).unwrap();

        let mut permissions = fs::metadata(&data_f).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&data_f, permissions).unwrap();

        // This should fail because Partition::try_new tries to open the data file in append mode
        let result = Partition::try_new(&data_f, &ptr_f);
        assert!(
            result.is_err(),
            "Should fail to open a read-only file for appending"
        );

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Permission denied"),
            "Expects permission denied error"
        );

        // Clean up: must set back to writable to delete on some OSs
        let mut permissions = fs::metadata(&data_f).unwrap().permissions();
        permissions.set_mode(0o644);
        let _ = fs::set_permissions(&data_f, permissions);
    }

    #[test]
    fn test_non_existent_directory() {
        // Path that cannot exist (nested in a non-existent folder)
        let data_f = PathBuf::from("/this/path/does/not/exist/data.bin");
        let ptr_f = PathBuf::from("/this/path/does/not/exist/ptr.bin");

        let result = Partition::try_new(&data_f, &ptr_f);
        assert!(result.is_err(), "Should fail when directory doesn't exist");
    }
}
