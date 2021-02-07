#![allow(unused_imports)]
use std::collections::HashMap;
use std::time::SystemTime;
//use thriftlike::*;

fn main() {
    /*
        // Simple benchmark for one Statistics blob from a fastparquet test case
        let data = b"\x18\x08\xf6\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00";
        let mut fo = FileObj::new(data);
        let mut out: HashMap<u8, AllTypes> = HashMap::new();

        let now = SystemTime::now();
        let n = 1000000;
        for _ in 0..n {
            fo.seek(0, Whence::Start);
            out = read_struct(&mut fo);
        }
        // python run: 6.96 µs (including making class)
        // rust debug run: 3.914 µs (including text output)
        // rust release run: 0.36 µs (including text output)

        println!("{:?}", out);

        println!("{:?}", now.elapsed().unwrap().as_millis());

        let data = b"\x18Ncat=fred/catnum=1/part-r-00000-4805f816-a859-4b75-8659-285a6617386f.gz.parquet\x16\x08\x1c\x15\x045\x04\x16T\x16\x82\x06\x16\xc6\x02&\x08<\x18\x08\xf6\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00";
        let mut fo = FileObj::new(data);

            fo.seek(0, Whence::Start);
            out = read_struct(&mut fo);
        }
        println!("{:?}", out);

        println!("{:?}", now.elapsed().unwrap().as_millis());
    // one column
    let data = b"\x18Ncat=fred/catnum=1/part-r-00001-4805f816-a859-4b75-8659-285a6617386f.gz.parquet\x16\x08\x1c\x15\x04\x195\x00\x08\x06\x19\x18\x03num\x15\x04\x16T\x16\x82\x06\x16\xce\x02&\x08<\x18\x08\xf2\x01\x00\x00\x00\x00\x00\x00\x18\x08\xfc\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00";
    let mut fo = FileObj::new(data);
    let out = read_struct(&mut fo);
    println!("{:?}", out);

    // full row group

    let data = b"\x19\x1c\x18Ncat=fred/catnum=1/part-r-00000-4805f816-a859-4b75-8659-285a6617386f.gz.parquet\x16\x08\x1c\x15\x04\x195\x00\x08\x06\x19\x18\x03num\x15\x04\x16T\x16\x82\x06\x16\xc6\x02&\x08<\x18\x08\xf6\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00\x16\x82\x06\x16T\x00";
    let mut fo = FileObj::new(data);
    let now = SystemTime::now();
    let n = 1000000;
    let mut out: HashMap<u8, AllTypes> = HashMap::new();
    for _ in 0..n {
        fo.seek(0, Whence::Start);
        out = read_struct(&mut fo);
    }
    println!("{:?}", out);
    println!("{:?}", now.elapsed().unwrap().as_millis());
    */
}
