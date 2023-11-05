// Rust code (src/lib.rs)
mod structual;

use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use pyo3::prelude::*;
use inflate::inflate_bytes_zlib;
use quick_xml::Reader;
use quick_xml::events::Event;
use chrono::NaiveDateTime;
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

    fn process_bytes_zlib(&self, chunk: u32, content_: &[u8], str_content_: &[u8]) {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        let decompress = inflate_bytes_zlib(&content).unwrap();
        let decompress_str = inflate_bytes_zlib(&str_content).unwrap();
        self._process_bytes(chunk, decompress, decompress_str);
    }

    fn process_bytes(&self, chunk: u32, content_: &[u8], str_content_: &[u8]) {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        self._process_bytes(chunk, content, str_content)
    }

    fn _process_bytes(&self, chunk: u32, content: Vec<u8>, str_content: Vec<u8>) {

        let name_resolve = str_resolve(str_content);
        let mut buffer = Vec::new();
        let mut c_list: Vec<String> = Vec::new();
        let mut row_a: Vec<Option<StructCsv>> = Vec::new();
        let mut is_v = false;
        let mut s: Option<StructCsv> = None;

        // tag "dimension ref=\"xx:yy\"/" の属性取得
        let dimension_tag = vec![100, 105, 109, 101, 110, 115, 105, 111, 110];

        let navi = create_navi();
        let tx1 = mpsc::Sender::clone(&self.pro);
        let mut count = 0;
        let closure = move || {
            let mut width_len: Option<usize> = None;
            let mut xml_reader = Reader::from_reader(content.as_ref());
            loop {
                match xml_reader.read_event_into(&mut buffer) {
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
                                                    let b = column_to_number(a);
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
                                count += 1;
                                if count == chunk {
                                    let val = c_list.join("\n");
                                    tx1.send(val).unwrap();
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
                        let val = c_list.join("\n");
                        tx1.send(val).unwrap();
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                    _ => {
                        if buffer.starts_with(&dimension_tag){
                            let dim_tag = String::from_utf8_lossy(&buffer).into_owned();
                            let dim_tag_last = dim_tag.split(":").last().unwrap();
                            let idx_num = column_to_number(dim_tag_last.to_string());
                            row_a = vec![None; idx_num];
                            width_len = Some(idx_num);
                        }
                    }
                }
                buffer.clear();
            }
            tx1.send(String::from("finish")).unwrap();
        };
        thread::spawn(closure);
    }

    fn generate_data_chunk(&mut self) -> PyResult<String> {
        let csv_data = self.con.recv().unwrap();
        Ok(csv_data)
    }
}

fn str_resolve(content: Vec<u8>) ->  Vec<String> {
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
                    let val = e.unescape().unwrap().into_owned();
                    name_resolve.push(val);
                    is_text = false;
                    no_text_ = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
            _ => {}
        }
        buffer.clear();
    }
    name_resolve
}

fn column_to_number(s: String) -> usize {
    let column_index= s.to_uppercase().chars().into_iter().
        filter(|a| a.is_ascii_uppercase()).
        map(|a| a as usize - 64).
        reduce(|acc, x| acc * 26 + x).unwrap_or(0);
    column_index
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
