// Rust code (src/lib.rs)
mod structual;

use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use pyo3::prelude::*;

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

    fn process_bytes(&self, chunk: u32, content_: &[u8], str_content_: &[u8]) {

        let content = content_.to_vec();
        let str_content = str_content_.to_vec();

        let name_resolve = str_resolve(str_content);
        let mut buffer = Vec::new();
        let mut c_list: Vec<String> = Vec::new();
        let mut row: Vec<StructCsv> = Vec::new();
        let mut is_v = false;
        let mut s: Option<StructCsv> = None;
        let target_attr = vec![115u8, 116u8];  // b"s", b"t"
        let navi = create_navi();
        let tx1 = mpsc::Sender::clone(&self.pro);
        let mut count = 0;
        let closure = move || {
            let mut xml_reader = Reader::from_reader(content.as_ref());
            loop {
                match xml_reader.read_event_into(&mut buffer) {
                    Ok(Event::Start(e)) => {
                        match e.name().as_ref() {
                            b"c" => {
                                for i in e.attributes() {
                                    match i {
                                        Ok(x) => {
                                            let a = if target_attr.
                                                contains(&x.key.into_inner()[0]) {
                                                x.key.into_inner()[0].clone()
                                            } else { 0u8 };
                                            s = Some(StructCsv::new(a));
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
                                let i = row.into_iter().map(|a| {
                                    a.clone().get_value(&navi, &name_resolve)
                                }).collect::<Vec<String>>().join(",");
                                c_list.push(i);
                                count += 1;
                                if count == chunk {
                                    let val = c_list.join("\n");
                                    tx1.send(val).unwrap();
                                    count = 0;
                                    c_list = Vec::new();
                                }
                                row = Vec::new();
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
                                    row.push(v.clone());
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
                    _ => {}
                }
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
    }
    name_resolve
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
