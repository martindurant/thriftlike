use std::collections::HashMap;
// use std::option::Option;
use std::time::SystemTime;
// use bytes::Bytes;
use byteorder::{BigEndian, ReadBytesExt};

fn main() {
    // Simple benchmark for one Statistics blob from a fastparquet test case
    let data = b"\x18\x08\xf6\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00";
    let mut fo = FileObj::new(data);
    let mut out: HashMap<u8, AllTypes> = HashMap::new();

    let now = SystemTime::now();
    let n = 1000000;
    for _ in 0..n {
        fo.seek(0, 0);
        out = read_struct(&mut fo);
    }
    // python run: 6.96 µs (including making class)
    // rust debug run: 3.914 µs (including text output)
    // rust release run: 0.36 µs (including text output)

    println!("{:?}", out);

    println!("{:?}", now.elapsed().unwrap().as_millis());
}

// The parquet spec has no MAP or UNION
// (except ColumnOrder, which only has one field, which has no value;
// this amounts to bool)

// In memory byte buffer with file-like API
struct FileObj<'a> {
    data: &'a [u8],
    loc: usize,
    size: usize,
}

impl FileObj<'_> {
    // Read one byte and return it
    fn read_byte(&mut self) -> u8 {
        let out = self.data[self.loc];
        self.loc += 1;
        out
    }

    // Read n bytes as a reference to the array
    fn read(&mut self, n: usize) -> &[u8] {
        self.loc += n;
        &self.data[self.loc - n..self.loc]
    }

    // Create FileObj from data
    fn new(data: &[u8]) -> FileObj {
        FileObj {
            data,
            loc: 0,
            size: data.len(),
        }
    }

    // reset file location
    fn seek(&mut self, to: usize, whence: u8) -> usize {
        match whence {
            0 => self.loc = to,
            1 => self.loc += to,
            2 => self.loc = self.size - to,
            _ => println!("bad seek"),
        }
        self.loc
    }
}

fn read_unsigned_var_int(file_obj: &mut FileObj) -> u64 {
    let mut result: u64 = 0;
    let mut shift: u8 = 0;
    let mut byte: u8;
    loop {
        byte = file_obj.read_byte();
        result |= ((byte & 0x7F) << shift) as u64;
        if (byte & 0x80) == 0 {
            break;
        };
        shift += 7;
    }
    result
}

fn int_zigzag(n: i32) -> u64 {
    ((n << 1) ^ (n >> 31)) as u64
}

fn zigzag_int(n: u64) -> i32 {
    (n as i32 >> 1) ^ -(n as i32 & 1)
}

/*
fn long_zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn zigzag_long(n: u64) -> i64 {
    (n as i64 >> 1) ^ -(n as i64 & 1)
}
*/

#[derive(Debug)]
enum AllTypes {
    Bool(bool),
    I64(i64),
    I32(i32),
    Binary(Vec<u8>),
    Struct(HashMap<u8, AllTypes>),
    List(Vec<AllTypes>),
    Map(HashMap<AllTypes, AllTypes>),
}

fn read_struct(file_obj: &mut FileObj) -> HashMap<u8, AllTypes> {
    let mut byte: u8;
    let mut id: u8 = 0;
    let mut typ: u8;
    let mut out: HashMap<u8, AllTypes> = HashMap::new();
    loop {
        byte = file_obj.read_byte();
        if byte == 0 {
            // stop field, end of struct
            break;
        };
        if byte & 0b11110000 > 0 {
            // short form: delta ID
            id += (byte & 0b11110000) >> 4
        } else {
            // long form: absolute ID value
            id = int_zigzag(file_obj.read(2).read_i16::<BigEndian>().unwrap() as i32) as u8
        }
        typ = byte & 0b00001111;
        match typ {
            1 => out.insert(id, AllTypes::Bool(true)),
            2 => out.insert(id, AllTypes::Bool(false)),
            6 => out.insert(
                id,
                AllTypes::I32(
                    // file_obj.read(4).read_i32::<BigEndian>().unwrap()
                    zigzag_int(read_unsigned_var_int(file_obj)),
                ),
            ),
            8 => {
                let val = read_unsigned_var_int(file_obj);
                out.insert(
                    id,
                    AllTypes::Binary(Vec::<u8>::from(file_obj.read(val as usize))),
                )
            }
            _ => None,
        };
    }
    out
}
