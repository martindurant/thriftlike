use std::collections::HashMap;
use std::time::{Duration, SystemTime};
// use bytes::Bytes;
// use byteorder::{BigEndian, ReadBytesExt};

fn main() {
    let data = b"\x18\x08\xf6\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00";
    let mut fo = FileObj::new(data);
    let out = read_unsigned_var_int(&mut fo);
    println!("{}", out);

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
        if whence == 0 {
            self.loc = to
        };
        if whence == 1 {
            self.loc += to
        };
        if whence == 2 {
            self.loc = self.size - to
        };
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

fn long_zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn zigzag_long(n: u64) -> i64 {
    (n as i64 >> 1) ^ -(n as i64 & 1)
}

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
            break;
        };
        if byte & 0b11110000 > 0 {
            id += (byte & 0b11110000) >> 4;
        };
        typ = byte & 0b00001111;
        if typ == 1 {
            out.insert(id, AllTypes::Bool(true));
        }
        if typ == 1 {
            out.insert(id, AllTypes::Bool(false));
        }
        if typ == 6 {
            out.insert(
                id,
                AllTypes::I32(
                    // file_obj.read(4).read_i32::<BigEndian>().unwrap()
                    zigzag_int(read_unsigned_var_int(file_obj)),
                ),
            );
        }
        if typ == 8 {
            let len = read_unsigned_var_int(file_obj);
            out.insert(
                id,
                AllTypes::Binary(Vec::<u8>::from(file_obj.read(len as usize))),
            );
        }
    }
    out
}
