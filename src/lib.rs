// Rust code (src/lib.rs)
mod structual;

use std::io::Read;
use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyException};
use inflate::inflate_bytes_zlib;
use quick_xml::Reader;
use quick_xml::events::{BytesText, Event};
use chrono::NaiveDateTime;
use flate2::read::ZlibDecoder;

use crate::structual::StructCsv;


#[pyclass]
struct DataGenerator {
    pro: Sender<String>,
    con: Receiver<String>
}

#[pymethods]
impl DataGenerator  {
    #[new]
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        DataGenerator {
            pro: tx,
            con: rx
        }
    }

    fn process_bytes_zlib(&self, chunk: u32, content_: &[u8], str_content_: &[u8]) -> PyResult<()> {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        let e_msg = "failed to decompress content";
        let decompress_str = inflate_bytes_zlib(&str_content).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        self._process_bytes(chunk, content, decompress_str)
    }

    fn process_bytes(&self, chunk: u32, content_: &[u8], str_content_: &[u8]) -> PyResult<()> {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        self._process_bytes(chunk, content, str_content)
    }

    fn _process_bytes(&self, chunk: u32, content: Vec<u8>, str_content: Vec<u8>) -> PyResult<()> {

        let name_resolve = str_resolve(str_content)?;
        // let mut buffer = Vec::new();
        let mut c_list: Vec<String> = Vec::new();
        let mut row_a: Vec<Option<StructCsv>> = Vec::new();
//        let mut row_a: Vec<Option<StructCsv>> = vec![None; 8];
        let mut is_v = false;
        let mut s: Option<StructCsv> = None;

        // tag "dimension ref=\"xx:yy\"/" の属性取得
        let dimension_tag = vec![100, 105, 109, 101, 110, 115, 105, 111, 110];
        let target_terminal_vec: Vec<u8> = vec![60, 47, 114, 111, 119, 62]; // </row> tag

        let navi = create_navi();
        let tx1 = mpsc::Sender::clone(&self.pro);
        let mut count = 0;
        let closure = move || -> Result<(), PyErr>{
            let mut first_out = Vec::<u8>::new();
            let mut second_out = Vec::<u8>::new();
            let mut decoder = ZlibDecoder::new(content.as_slice());
            let target_len = 1000;
            let mut is_upper = true;
            let mut is_last = false;
            let mut width_len: Option<usize> = None;
            //let mut width_len: Option<usize> = Some(8);
            // let mut buffer_inner = Vec::new();
            //let mut xml_reader: Option<Reader<_>> = None;
            //let mut x: Option<Vec<u8>> = None;
            loop {

                if is_last{
                    let val = c_list.join("\n");
                    if let Err(e) = tx1.send(val) {
                        let msg = format!("failed to send message: {}", e);
                        return Err(PyValueError::new_err(msg));
                    };
                    tx1.send(String::from("finish")).unwrap();
                    break;
                }

                let mut buffer_outer = vec![0; target_len];
                let bytes_read = match decoder.read(&mut buffer_outer) {
                    Ok(x) => {
                        //println!("buffer_outer = {:?}", String::from_utf8_lossy(&buffer_outer));
                        x
                    }
                    Err(e) => {
                        println!("e = {:?}", e);
                        0
                    }
                };
                //println!("bytes_read = {:?}", &bytes_read);
                // let bytes_read = decoder.read(&mut buffer_outer).unwrap();

                let x: Option<Vec<u8>> = if bytes_read == 0 {
                    let a = if is_upper {
                        first_out.clone()
                    }else {
                        second_out.clone()
                    };
                    is_last = true;
                    let op_index = find_vec_index_rev(&a, &target_terminal_vec);
                    match op_index {
                        Some(i) => {
                            let b = &a[0..i+6];
                            Some(b.to_vec())
                        },
                        None => None
                    }
                }
                else if let Some(index) = find_vec_index_rev(&buffer_outer, &target_terminal_vec) {
                    if is_upper {
                        first_out.extend(&buffer_outer[0..&index+6]);
                        second_out = Vec::<u8>::new();
                        second_out.extend(&buffer_outer[&index+6..bytes_read]);
                        is_upper = false;
                        Some(first_out.clone())
                    }else{
                        second_out.extend(&buffer_outer[0..&index+6]);
                        first_out = Vec::<u8>::new();
                        first_out.extend(&buffer_outer[&index+6..bytes_read]);
                        is_upper = true;
                        Some(second_out.clone())
                    }
                } else {
                    if is_upper {
                        first_out.extend(&buffer_outer[0..bytes_read]);
                    }else{
                        second_out.extend(&buffer_outer[0..bytes_read]);
                    }
                    continue;
                };

                if let Some(x) = &x {
                    //println!("pre inner = {:?}", String::from_utf8_lossy(&x));
                    let mut buffer_inner = Vec::new();
                    let mut xml_reader = Reader::from_reader(x.as_ref());
                    // let mut buffer_inner = Vec::new();
                    loop {
                        match xml_reader.read_event_into(&mut buffer_inner) {
                            Ok(Event::Start(e)) => {
                                match e.name().as_ref() {
                                    b"c" => {
                                        let mut struct_csv = StructCsv::new();
                                        for i in e.attributes() {
                                            match i {
                                                Ok(x) => {
                                                    match &x.key.into_inner() {
                                                        [115u8] | [116u8] => { // b"s", b"t"
                                                            struct_csv.set_attr(
                                                                x.key.into_inner()[0].clone());
                                                        }
                                                        [114u8] => { // b"r"
                                                            let a = String::from_utf8_lossy(
                                                                &*x.clone().value.into_owned()).
                                                                into_owned();
                                                            let b = column_to_number(a)?;
                                                            struct_csv.set_r_attr_v(b - 1);
                                                        }
                                                        _ => {}
                                                    }
                                                    s = Some(struct_csv.clone());

                                                }
                                                Err(_) => {}
                                            }
                                        }
                                    },
                                    b"v" => is_v = true,
                                    _ => {},
                                }
                            }
                            Ok(Event::End(e)) => {
                                match e.name().as_ref() {
                                    b"row" => {
                                        let i = row_a.into_iter().map(|a| {
                                            match a {
                                                Some(s) => s.clone().
                                                    get_value(&navi, &name_resolve),
                                                None => "".to_string()
                                            }
                                        }).collect::<Vec<String>>().join(",");
                                        c_list.push(i);
                                        // println!("loop chunk = {:?}", chunk);
                                        // println!("loop count = {:?}", count);
                                        // println!("loop c_list = {:?}", c_list);
                                        count += 1;
                                        if count == chunk {
                                            let val = c_list.join("\n");
                                            if let Err(e) = tx1.send(val) {
                                                let msg = format!("failed to send message: {}", e);
                                                return Err(PyValueError::new_err(msg));
                                            };
                                            count = 0;
                                            c_list = Vec::new();
                                        }
                                        row_a = vec![None; width_len.unwrap()];
                                    },
                                    _ => {},
                                }
                            }
                            Ok(Event::Text(e)) => {
                                if is_v {
                                    match s {
                                        Some(ref mut v) => {
                                            let val = e.unescape().unwrap().into_owned();
                                            v.set_value(val);
                                            let i = v.get_r_attr_v();
                                            row_a[i] = Some(v.clone());
                                            s = None;
                                        }
                                        None => {}
                                    }
                                    is_v = false;
                                }
                            }
                            Ok(Event::Eof) => {
                                // let val = c_list.join("\n");
                                // if let Err(e) = tx1.send(val) {
                                //     let msg = format!("failed to send message: {}", e);
                                //     return Err(PyValueError::new_err(msg));
                                // };
                                break;
                            }
                            Err(e) => {
                                let msg = format!("something wrong: {}", e);
                                return Err(PyException::new_err(msg));
                            }
                            _ => {
                                if buffer_inner.starts_with(&dimension_tag){
                                    //println!("dimension_tag yes");
                                    let dim_tag = String::from_utf8_lossy(&buffer_inner).into_owned();
                                    let dim_tag_contains_colon = dim_tag.contains(':');
                                    let dim_tag_last = if dim_tag_contains_colon {
                                        dim_tag.split(":").last().unwrap()
                                    } else {
                                        let msg = format!("wrong dimension tag");
                                        return Err(PyValueError::new_err(msg));
                                    };
                                    let idx_num = column_to_number(dim_tag_last.to_string())?;
                                    row_a = vec![None; idx_num];
                                    width_len = Some(idx_num);
                                }
                            }
                        }
                        //println!("loop inner = {:?}", String::from_utf8_lossy(&buffer_inner));
                        buffer_inner.clear();
                    }
                }
            }
            if let Err(e) = tx1.send(String::from("finish")) {
                let msg = format!("failed to send message: {}", e);
                return Err(PyValueError::new_err(msg))
            };
            Ok(())
        };
        thread::spawn(closure);
        Ok(())
    }

    fn generate_data_chunk(&mut self) -> PyResult<String> {
        let e_msg = "failed to recv message";
        let data = self.con.recv().
            map_err(|e| PyValueError::new_err(format!("{}: {}", e_msg, e)))?;
        Ok(data)
    }
}

fn str_resolve(content: Vec<u8>) ->  Result<Vec<String>, PyErr> {
    // excelの名称解決
    let mut xml_reader = Reader::from_reader(content.as_ref());
    let mut buffer = Vec::new();
    let mut name_resolve: Vec<String> = Vec::new();
    let mut is_text = false;
    let mut no_text_ = false;
    loop {
        match xml_reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => {
                match e.name().as_ref() {
                    b"t" => is_text = true,
                    b"rPh" => no_text_ = true,
                    _ => no_text_ = false,
                }
            }
            Ok(Event::Text(e)) => {
                if &is_text & !&no_text_ {
                    let val = common_match_fn(e)?;
                    name_resolve.push(val);
                    is_text = false;
                    no_text_ = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                let msg = format!("something wrong: {}", e);
                return Err(PyException::new_err(msg));
            }
            _ => {}
        }
        buffer.clear();
    }
    Ok(name_resolve)
}

fn common_match_fn(e: BytesText) -> Result<String, PyErr> {
    let val = match e.unescape() {
        Ok(v) => {v.into_owned()}
        Err(err) => {
            let msg = format!("wrong BytesText: {}", err);
            return Err(PyValueError::new_err(msg))
        }
    };
    Ok(val)
}

fn find_vec_index_rev<T: PartialEq>(vector: &[T], sub_vec: &[T]) -> Option<usize> {
    let s_vec_len = sub_vec.len();

    for i in (s_vec_len..=vector.len()).rev() {
        if i >= s_vec_len && &vector[(i - &s_vec_len)..i] == sub_vec {
            return Some(&i - &s_vec_len)
        }
    }
    None
}

fn column_to_number(s: String) -> Result<usize, PyErr> {
    let e_msg = "Error: Unable to convert column to number";
    let column_index= s.to_uppercase().chars().into_iter().
        filter(|a| a.is_ascii_uppercase()).
        map(|a| a as usize - 64).
        reduce(|acc, x| acc * 26 + x).
        ok_or(PyValueError::new_err(e_msg))?;
    Ok(column_index)
}

fn create_navi() -> NaiveDateTime {
    NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(1899, 12, 30).unwrap(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

/// A Python module implemented in Rust.
#[pymodule]
fn xlsxgen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DataGenerator>()?;
    Ok(())
}
