// Rust code (src/lib.rs)
mod structual;

use std::collections::HashMap;
use indexmap::IndexMap;
use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyException};
use inflate::inflate_bytes_zlib;
use quick_xml::Reader;
use quick_xml::events::{BytesText, Event};
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

    fn process_bytes_zlib(&self, chunk: u32, content_: &[u8], str_content_: &[u8],
                          stle_content_: &[u8]) -> PyResult<()> {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        let e_msg = "failed to decompress content";
        let decompress = inflate_bytes_zlib(&content).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        let decompress_str = inflate_bytes_zlib(&str_content).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        let decompress_stle = inflate_bytes_zlib(&stle_content_).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        self._process_bytes(chunk, decompress, decompress_str,
                            decompress_stle)
    }

    fn process_bytes(&self, chunk: u32, content_: &[u8], str_content_: &[u8],
                     stle_content_: &[u8]) -> PyResult<()> {
        let content = content_.to_vec();
        let str_content = str_content_.to_vec();
        let stle_content = stle_content_.to_vec();
        self._process_bytes(chunk, content, str_content, stle_content)
    }

    fn _process_bytes(&self, chunk: u32, content: Vec<u8>, str_content: Vec<u8>,
                      stle_content: Vec<u8>) -> PyResult<()> {

        let style_vec = stle_resolve(stle_content)?;
        let style_resolve = date_ident(style_vec)?;
        let name_resolve = str_resolve(str_content)?;
        let mut buffer = Vec::new();
        let mut c_list: Vec<String> = Vec::new();
        let mut row_a: Vec<Option<StructCsv>> = Vec::new();
        let mut is_v = false;
        let mut s: Option<StructCsv> = None;

        // tag "dimension ref=\"xx:yy\"/" の属性取得
        let dimension_tag = b"dimension";

        let navi = create_navi();
        let tx1 = mpsc::Sender::clone(&self.pro);
        let mut count = 0;
        let closure = move || -> Result<(), PyErr>{
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
                                            match x.key.into_inner() {
                                                b"s" => {
                                                    struct_csv.set_s_attr(
                                                        x.key.into_inner().to_vec());
                                                    let msg = "structual parse wrong";
                                                    let a = String::from_utf8(x.value.to_vec()).
                                                        map_err(|e| PyValueError::new_err(format!("{}: {}", msg, e)))?.
                                                        parse::<usize>().
                                                        map_err(|e| PyValueError::new_err(format!("{}: {}", msg, e)))?;
                                                    struct_csv.set_s_attr_v(a);
                                                }
                                                b"t" => {
                                                    struct_csv.set_t_attr(
                                                        x.key.into_inner().to_vec());
                                                }
                                                b"r" => {
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
                                        Some(s) => {
                                            let msg = "structual wrong";
                                            s.clone().
                                                get_value(&navi, &name_resolve, &style_resolve).
                                                map_err(|e| PyValueError::new_err(format!("{}: {}", msg, e)))
                                        },
                                        None => Ok("".to_string())
                                    }
                                }).collect::<Result<Vec<String>, PyErr>>()?.join(",");
                                c_list.push(i);
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
                                    let val = common_match_fn(e)?;
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
                        break;
                    }
                    Err(e) => {
                        let msg = format!("something wrong: {}", e);
                        return Err(PyException::new_err(msg));
                    }
                    _ => {
                        if buffer.starts_with(dimension_tag){
                            let dim_tag = String::from_utf8_lossy(&buffer).into_owned();
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
                buffer.clear();
            }
            let last_data = c_list.join("\n");
            let last_msg = String::from("finish");
            for v in vec![last_data, last_msg].into_iter(){
                if let Err(e) = tx1.send(v) {
                    let msg = format!("failed to send message: {}", e);
                    return Err(PyValueError::new_err(msg));
                };
            }
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

fn date_ident(style_resolve: (Vec<String>, HashMap<String, String>))
    -> Result<IndexMap<String, bool>, PyErr> {
    // style.xml cellXfsタグから情報を取得し日付判定mapを作成する
    let style_vec = style_resolve.0;
    let style_map = style_resolve.1;

    let resolve_map: Result<IndexMap<String, bool>, PyErr> =
        style_vec.into_iter().map(|num_fmt_str|{

        let msg = "style parse wrong";
        let num_fmt_id = num_fmt_str.parse::<u32>().
            map_err(|e| PyValueError::new_err(format!("{}: {}", msg, e)))?;
        let bool_value = match num_fmt_id {
            // numFmtの参考 ↓
            // https://learn.microsoft.com/ja-jp/dotnet/api/documentformat.openxml.spreadsheet.numberingformat?view=openxml-2.8.1
            0..=4 | 9..=13 | 37..=40 | 45..=49 => false,
            14..=22 => true,
            _ => {
                // numFmtの例外の場合はstyleに y が２個以上, m が１個以上の場合は trueとしている
                let bool_ = match style_map.get(&num_fmt_str) {
                    Some(s) if s.matches('y').count() >= 2 &&
                        s.matches('m').count() >= 1 => true,
                    _ => false
                };
                bool_
            },
        };
        Ok((num_fmt_str, bool_value))
    }).collect();
    resolve_map
}

fn stle_resolve(content: Vec<u8>) ->  Result<(Vec<String>, HashMap<String, String>), PyErr> {
    // excelのstyle解決
    let mut xml_reader = Reader::from_reader(content.as_ref());
    let mut buffer = Vec::new();
    let mut inner_buf = Vec::new();
    let mut style_vec: Vec<String> = Vec::new();
    let mut style_map: HashMap<String, String> = HashMap::new();

    // target tag の属性取得
    let numfmt_tag = b"numFmt ";
    let xf_tag = b"xf ";
    loop {
        match xml_reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"cellXfs" => loop {
                inner_buf.clear();
                match xml_reader.read_event_into(&mut inner_buf) {
                    Ok(Event::End(ref e)) if e.local_name().as_ref() == b"cellXfs" => break,
                    _ => {
                        if inner_buf.starts_with(xf_tag){
                            let xf_tag_str = String::from_utf8_lossy(&inner_buf).into_owned();
                            let msg = "failed xf style ";
                            let target_attr_xf = "numFmtId=\"";
                            let target_end = "\"";
                            let num_fmt_id = extract_target_str(
                                &xf_tag_str, target_attr_xf, target_end, msg)?;
                            style_vec.push(num_fmt_id);
                        }
                    },
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                let msg = format!("something style wrong: {}", e);
                return Err(PyException::new_err(msg));
            }
            _ => {
                if buffer.starts_with(numfmt_tag){
                    let numfmt_tag_str = String::from_utf8_lossy(&buffer).into_owned();
                    let msg = "failed numfmt style ";
                    let target_attr_numfmt = "numFmtId=\"";
                    let target_attr_fmtcode = "formatCode=\"";
                    let target_end = "\"";
                    let num_fmt_id = extract_target_str(
                        &numfmt_tag_str, target_attr_numfmt, target_end, msg)?;
                    let format_code = extract_target_str(
                        &numfmt_tag_str, target_attr_fmtcode, target_end, msg)?;
                    style_map.insert(num_fmt_id, format_code);
                }
            }
        }
        buffer.clear();
    }
    Ok((style_vec, style_map))
}

fn extract_target_str(target_tag: &str, target_attr: &str, target_end: &str, error_msg: &str)
                      -> Result<String, PyErr> {
    let num_fmt_id_start = target_tag.find(target_attr).
        ok_or(PyValueError::new_err(format!("{}", error_msg)))? + target_attr.len();
    let num_fmt_id_end = target_tag[num_fmt_id_start..].find(target_end).
        ok_or(PyValueError::new_err(format!("{}", error_msg)))? + num_fmt_id_start;
    Ok(target_tag[num_fmt_id_start..num_fmt_id_end].to_string())
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
