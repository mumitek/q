use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

fn from_bytes(bytes: &[u8]) -> usize {
    ((bytes[0] as usize) << 24)
        | ((bytes[1] as usize) << 16)
        | ((bytes[2] as usize) << 8)
        | (bytes[3] as usize)
}

fn to_bytes(size: usize) -> Vec<u8> {
    vec![
        (size >> 24) as u8,
        ((size << 8) >> 24) as u8,
        ((size << 16) >> 24) as u8,
        ((size << 24) >> 24) as u8,
    ]
}
struct PartitionReader {
    pointer: usize,
    reader: BufReader<File>,
    pointer_writer: File,
}

pub(crate) struct Partition {
    reader: Mutex<PartitionReader>,
    writer: Mutex<File>,
}

fn read_pointer(pointer_file_name: &str) -> Result<(File, usize), Box<dyn Error>> {
    let path = Path::new(pointer_file_name);
    let pointer = if path.is_file() {
        let mut reader = File::open(path)?;
        let mut buffer = vec![0u8; 4];
        let read = reader.read(&mut buffer)?;
        if read != buffer.len() {
            return Err("Couldn't read whole pointer file".into());
        }
        from_bytes(&buffer)
    } else {
        0
    };
    let mut writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let pointer_bytes = to_bytes(pointer);
    writer.set_len(0)?;
    let wrote = writer.write(&pointer_bytes)?;
    if wrote != pointer_bytes.len() {
        return Err("Failed to write all the bytes while initialising pointer pointer".into());
    }
    Ok((writer, pointer))
}

impl PartitionReader {
    pub fn try_new(file_name: &str, pointer_file_name: &str) -> Result<Self, Box<dyn Error>> {
        let (pointer_writer, pointer) = read_pointer(pointer_file_name)?;
        let mut reader = File::open(file_name)?;
        reader.seek(SeekFrom::Start(pointer as u64))?;
        let reader = BufReader::new(reader);
        Ok(Self {
            pointer,
            pointer_writer,
            reader,
        })
    }

    pub fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut message_size_buffer = vec![0u8, 0, 0, 0];
        let read = self.reader.read(&mut message_size_buffer)?;
        if read != message_size_buffer.len() {
            return Err("Not enough bytes returned when reading messages size".into());
        }

        let message_size = from_bytes(&message_size_buffer);
        let mut message_buffer = vec![0u8; message_size];

        let read = self.reader.read(&mut message_buffer)?;

        if read != message_size {
            return Err("Not enough bytes returned when reading message".into());
        }

        self.update_pointer(message_size + message_size_buffer.len())?;
        Ok(message_buffer)
    }

    fn update_pointer(&mut self, increment: usize) -> Result<(), Box<dyn Error>> {
        self.pointer += increment;
        self.pointer_writer.set_len(0)?;
        self.pointer_writer.seek(SeekFrom::Start(0))?;
        let message_size_buffer = to_bytes(self.pointer);
        let written = self.pointer_writer.write(&message_size_buffer)?;

        if written != message_size_buffer.len() {
            return Err("Failed to write message size buffer".into());
        }
        Ok(())
    }
}

impl Partition {
    pub(crate) fn try_new(data_file: &str, pointer_file: &str) -> Result<Self, Box<dyn Error>> {
        let writer = OpenOptions::new()
            .append(true)
            .create(true)
            .open(data_file)?;
        let reader = PartitionReader::try_new(data_file, pointer_file)?;
        Ok(Partition {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
        })
    }

    pub(crate) fn next(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut reader = self.reader.lock().unwrap();
        reader.read()
    }

    pub(crate) fn add(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut writer = self.writer.lock().unwrap();
        let size = to_bytes(bytes.len());
        let wrote = writer.write(&size)?;
        if wrote != size.len() {
            return Err("Couldn't write all of message size".into());
        }
        let wrote = writer.write(bytes)?;
        if wrote != bytes.len() {
            return Err("Couldn't write all of the message".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::fs;

    // Helper to get unique filenames for each test to avoid interference
    fn get_temp_files(id: &str) -> (String, String) {
        (format!("data_{}.bin", id), format!("pointer_{}.bin", id))
    }

    // Helper to remove files if they exist
    fn cleanup(files: Vec<String>) {
        for f in files {
            let _ = fs::remove_file(f);
        }
    }

    #[test]
    fn test_conversions() {
        let test_cases = vec![0, 1, 1024, 16777215, 4294967295];
        for &original_size in &test_cases {
            let bytes = to_bytes(original_size);
            let recovered_size = from_bytes(&bytes);

            assert_eq!(
                original_size, recovered_size,
                "Bit-shift conversion failed for {}",
                original_size
            );
            assert_eq!(bytes.len(), 4);
        }
    }

    #[test]
    fn test_partition_basic_io() {
        let (data_f, ptr_f) = get_temp_files("basic_io");

        // Setup initial files (assuming .create(true) isn't in your source yet)
        fs::write(&data_f, []).expect("Failed to setup data file");
        fs::write(&ptr_f, vec![0u8; 4]).expect("Failed to setup pointer file");

        let mut partition =
            Partition::try_new(&data_f, &ptr_f).expect("Failed to initialize partition");

        let msg = b"Hello Rust!";
        partition.add(msg).expect("Failed to write message");

        let read_msg = partition.next().expect("Failed to read message back");

        assert_eq!(
            msg.to_vec(),
            read_msg,
            "Read data does not match written data"
        );

        cleanup(vec![data_f, ptr_f]);
    }

    #[test]
    fn test_pointer_persistence() {
        let (data_f, ptr_f) = get_temp_files("persistence");
        fs::write(&data_f, []).expect("Setup failed");
        fs::write(&ptr_f, vec![0u8; 4]).expect("Setup failed");

        let msg1 = b"First Message";
        let msg2 = b"Second Message";

        // Session 1: Write two, read one
        {
            let mut partition = Partition::try_new(&data_f, &ptr_f).expect("Init session 1 failed");
            partition.add(msg1).expect("Write 1 failed");
            partition.add(msg2).expect("Write 2 failed");

            let res = partition.next().expect("Read 1 failed");
            assert_eq!(res, msg1);
        } // partition dropped here

        // Session 2: Re-open. It should point to the second message automatically.
        {
            let mut partition = Partition::try_new(&data_f, &ptr_f).expect("Init session 2 failed");
            let res = partition
                .next()
                .expect("Read 2 failed - pointer was not persisted");
            assert_eq!(
                res, msg2,
                "Did not resume from the correct pointer position"
            );
        }

        cleanup(vec![data_f, ptr_f]);
    }

    #[test]
    fn test_empty_read_error() {
        let (data_f, ptr_f) = get_temp_files("empty_err");
        fs::write(&data_f, []).expect("Setup failed");
        fs::write(&ptr_f, vec![0u8; 4]).expect("Setup failed");

        let mut partition = Partition::try_new(&data_f, &ptr_f).expect("Init failed");

        let result = partition.next();

        // We EXPECT an error here, so we assert that it is indeed an Err
        assert!(
            result.is_err(),
            "Reading from an empty partition should return an Error"
        );

        // Optionally inspect the error message
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Not enough bytes"),
            "Error message was unexpected: {}",
            err_msg
        );

        cleanup(vec![data_f, ptr_f]);
    }

    #[test]
    fn test_cold_start_creates_files() {
        let data_f = "cold_start_data.bin";
        let ptr_f = "cold_start_ptr.bin";

        // 1. Cleanup: Ensure we are starting from scratch
        let _ = std::fs::remove_file(data_f);
        let _ = std::fs::remove_file(ptr_f);

        // 2. Try to initialize - this will panic with a helpful message if creation fails
        // Note: This requires .create(true) in your OpenOptions to pass!
        let mut partition = Partition::try_new(data_f, ptr_f)
            .expect("Failed to initialize partition with non-existent files");

        // 3. Add a message to the empty partition
        let msg = b"New Partition Data";
        partition.add(msg).expect("Failed to add first message");

        // 4. Verify file existence on disk
        assert!(
            std::path::Path::new(data_f).exists(),
            "Data file was not created"
        );
        assert!(
            std::path::Path::new(ptr_f).exists(),
            "Pointer file was not created"
        );

        // 5. Read back to confirm data integrity
        let result = partition
            .next()
            .expect("Failed to read back the first message");
        assert_eq!(result, msg, "The read data does not match the written data");

        // 6. Verify the pointer was updated (4 bytes for size + message length)
        let ptr_content =
            std::fs::read(ptr_f).expect("Could not read pointer file for verification");
        assert_eq!(
            ptr_content.len(),
            4,
            "Pointer file should be exactly 4 bytes {:?}",
            ptr_content,
        );

        let current_ptr = from_bytes(&ptr_content);
        assert_eq!(
            current_ptr,
            4 + msg.len(),
            "Pointer value is incorrect after first write"
        );

        // Final Cleanup
        let _ = std::fs::remove_file(data_f);
        let _ = std::fs::remove_file(ptr_f);
    }
}
