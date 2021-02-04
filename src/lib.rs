use std::collections::HashMap;
// use std::option::Option;
// use bytes::Bytes;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
//use std::io::Cursor;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

// The parquet spec has no BYTE, MAP or UNION
// (except ColumnOrder, which only has one field, which has no value;
// this amounts to bool)

// In memory byte buffer with file-like API
pub struct FileObj<'a> {
    data: &'a [u8], // set in constructor, freed along with instance
    loc: usize,
    size: usize,
}

pub enum Whence {
    // possible values for FileObj.seek()
    Start = 0,
    Relative = 1,
    End = 2,
}

impl FileObj<'_> {
    // Read one byte and return it
    pub fn read_byte(&mut self) -> u8 {
        let out = self.data[self.loc];
        self.loc += 1;
        out
    }

    // Read n bytes as a reference to the array
    pub fn read(&mut self, n: usize) -> &[u8] {
        self.loc += n;
        &self.data[self.loc - n..self.loc]
    }

    // Create FileObj from data
    pub fn new(data: &[u8]) -> FileObj {
        FileObj {
            data,
            loc: 0,
            size: data.len(),
        }
    }

    // reset file location
    pub fn seek(&mut self, to: usize, whence: Whence) -> usize {
        match whence {
            Whence::Start => self.loc = to,
            Whence::Relative => self.loc += to,
            Whence::End => self.loc = self.size - to,
        }
        self.loc
    }
}

fn read_unsigned_var_int(file_obj: &mut FileObj) -> u64 {
    let mut result: u64 = 0;
    let mut shift: u8 = 0;
    let mut byte: u8;
    byte = file_obj.read_byte();
    if byte < 0x80 {
        // short cut
        return byte as u64;
    }
    loop {
        result |= (byte as u64 & 0x7F) << shift;
        if (byte & 0x80) == 0 {
            return result;
        };
        shift += 7;
        byte = file_obj.read_byte();
    }
}

/*
fn int_zigzag(n: i32) -> u64 {
    ((n << 1) ^ (n >> 31)) as u64
}
*/

fn zigzag_int(n: u64) -> i32 {
    (n as i32 >> 1) ^ -(n as i32 & 1)
}

/*
fn long_zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}
*/

#[inline]
fn zigzag_long(n: u64) -> i64 {
    (n as i64 >> 1) ^ -(n as i64 & 1)
}

#[derive(Debug)]
pub enum AllTypes {
    Bool(bool),
    I64(i64),
    I32(i32),
    F64(f64),
    Binary(Vec<u8>),
    Struct(HashMap<u8, AllTypes>),
    List(Vec<AllTypes>),
}

pub fn read_struct(file_obj: &mut FileObj) -> HashMap<u8, AllTypes> {
    let mut byte: u8;
    let mut id: u8 = 0;
    let mut typ: u8;
    let mut out: HashMap<u8, AllTypes> = HashMap::with_capacity(13);
    loop {
        byte = file_obj.read_byte();
        if byte == 0 {
            // stop field, end of struct
            break;
        };
        if byte & 0b11110000 == 0 {
            // long form: absolute ID value
            id = zigzag_int(file_obj.read(2).read_i16::<BigEndian>().unwrap() as u64) as u8
        } else {
            // short form: delta ID
            id += (byte & 0b11110000) >> 4
        }
        typ = byte & 0b00001111;
        match typ {
            1 => out.insert(id, AllTypes::Bool(true)),
            2 => out.insert(id, AllTypes::Bool(false)),
            5 => out.insert(
                //int32
                id,
                AllTypes::I32(zigzag_int(read_unsigned_var_int(file_obj))),
            ),
            6 => out.insert(
                //int64
                id,
                AllTypes::I64(zigzag_long(read_unsigned_var_int(file_obj))),
            ),
            7 => out.insert(
                //float64
                id,
                AllTypes::F64(file_obj.read(2).read_f64::<LittleEndian>().unwrap()),
            ),
            8 => out.insert(
                // binary (string)
                id,
                AllTypes::Binary(read_bin(file_obj)),
            ),
            9 => out.insert(id, AllTypes::List(read_list(file_obj))),
            12 => out.insert(id, AllTypes::Struct(read_struct(file_obj))),
            _ => None,
        };
    }
    out
}

// read binary string
#[inline]
fn read_bin(file_obj: &mut FileObj) -> Vec<u8> {
    let val = read_unsigned_var_int(file_obj);
    Vec::<u8>::from(file_obj.read(val as usize))
}

// read list of whatever as a vec
// only need list(struct), list(int32), list(byte-strings)
fn read_list(file_obj: &mut FileObj) -> Vec<AllTypes> {
    let byte = file_obj.read_byte();
    let typ: u8 = byte & 0x0f;
    let size: usize;
    if byte > 239 {
        // long form
        size = read_unsigned_var_int(file_obj) as usize;
    } else {
        // short form (up to 14 values)
        size = ((byte & 0xf0) >> 4) as usize;
    }
    let mut out: Vec<AllTypes> = Vec::with_capacity(size);
    match typ {
        5 => {
            for _ in 0..size {
                out.push(AllTypes::I32(zigzag_int(read_unsigned_var_int(file_obj))))
            }
        }
        8 => {
            for _ in 0..size {
                out.push(AllTypes::Binary(read_bin(file_obj)))
            }
        }
        12 => {
            for _ in 0..size {
                out.push(AllTypes::Struct(read_struct(file_obj)))
            }
        }
        _ => {}
    }

    out
}

#[pymodule]
fn thriftlike(py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "parse")]
    fn parse_py(_py: Python, data: &[u8]) -> PyResult<bool> {
        let mut fo = FileObj::new(data);
        let out = read_struct(&mut fo);

        Ok(true) //format!("{:?}", out))
    }

    Ok(())
}
