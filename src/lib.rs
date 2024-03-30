// Rust code (src/lib.rs)
mod structual;

use std::io::Read;
use std::collections::HashMap;
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
use std::sync::Mutex;

use crate::structual::StructCsv;


#[pyclass]
struct DataGenerator {
    pro: Mutex<Sender<String>>,
    con: Mutex<Receiver<String>>,
    e_pro: Mutex<Sender<String>>,
    e_con: Mutex<Receiver<String>>,
}

#[pymethods]
impl DataGenerator  {
    #[new]
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let (tx_err, rx_err) = mpsc::channel();
        DataGenerator {
            pro: Mutex::new(tx),
            con: Mutex::new(rx),
            e_pro: Mutex::new(tx_err),
            e_con: Mutex::new(rx_err),
        }
    }

    fn process_bytes_zlib(&self, chunk: u32, content_: &[u8], str_content_: &[u8],
                          stle_content_: &[u8]) -> PyResult<()> {
        let e_msg = "failed to decompress content";
        let content = content_.to_vec();
        let decompress_str = inflate_bytes_zlib(str_content_).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        let decompress_stle = inflate_bytes_zlib(stle_content_).map_err(
            |e| PyValueError::new_err(format!("{}: {}", &e_msg, e)))?;
        self._process_bytes_zlib(chunk, content, decompress_str, decompress_stle)
    }

    fn _process_bytes_zlib(&self, chunk: u32, content: Vec<u8>, str_content: Vec<u8>, stle_content: Vec<u8>) -> PyResult<()> {

        let style_vec: (Vec<String>, HashMap<String, String>) = stle_resolve(stle_content)?;
        let style_resolve: Vec<(String, bool)> = date_ident(style_vec)?;
        let name_resolve = str_resolve(str_content)?;
        let mut c_list: Vec<String> = Vec::new();
        let target_terminal_vec: Vec<u8> = vec![60, 47, 114, 111, 119, 62]; // </row> tag

        let tx1 = self.pro.lock().unwrap().clone();
        let tx_err = self.e_pro.lock().unwrap().clone();
        let closure = move || -> Result<(), PyErr>{
            let mut first_out = Vec::<u8>::new();
            let mut second_out = Vec::<u8>::new();
            let mut decoder = ZlibDecoder::new(content.as_slice());
            let target_len = 1000;
            let mut is_upper = true;
            let mut width_len: Option<usize> = None;
            let mut is_last = false;

            let c_list2 = loop {

                if is_last {
                    break c_list;
                }

                let mut buffer_outer = vec![0; target_len];

                let bytes_read = match decoder.read(&mut buffer_outer) {
                    Ok(x) => x,
                    Err(e) => {
                        let msg = format!("something wrong: {}", e);
                        return Err(PyException::new_err(msg));
                    }
                };

                let x: Option<Vec<u8>> = if bytes_read == 0 {
                    let a = if is_upper {
                        &first_out
                    }else {
                        &second_out
                    };
                    is_last = true;
                    find_vec_index_rev(a, &target_terminal_vec).map(|index| a[0..index+6].to_vec())
                } else if let Some(index) = find_vec_index_rev(&buffer_outer, &target_terminal_vec) {

                    let (current_out, other_out) = if is_upper {
                        (&mut first_out, &mut second_out)
                    } else {
                        (&mut second_out, &mut first_out)
                    };
                    current_out.extend(&buffer_outer[0..&index + 6]);
                    other_out.clear();
                    other_out.extend(&buffer_outer[&index + 6..bytes_read]);
                    is_upper = !is_upper;
                    Some(current_out.clone())
                } else {
                    let current_out = if is_upper { &mut first_out } else { &mut second_out };
                    current_out.extend(&buffer_outer[0..bytes_read]);
                    continue;
                };

                if let Some(x) = &x {
                    if width_len.is_none() {
                        let xml_reader = Reader::from_reader(x.as_ref());
                        width_len = common_xml_length(xml_reader, tx_err.clone())?;
                    }
                    let xml_reader = Reader::from_reader(x.as_ref());
                    c_list = common_xml_handler(xml_reader, tx1.clone(), tx_err.clone(),
                                                      &style_resolve, &name_resolve, c_list, &width_len, &chunk)?;
                }
            };
            let last_data = c_list2.join("\n");
            let last_msg = String::from("finish");
            for v in vec![last_data, last_msg].into_iter(){
                if let Err(e) = tx1.send(v) {
                    let msg = format!("failed to send message: {}", e);
                    let _ = tx_err.send(msg);
                };
            }
            Ok(())
        };
        thread::spawn(closure);
        Ok(())
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

        let style_vec: (Vec<String>, HashMap<String, String>) = stle_resolve(stle_content)?;
        let style_resolve: Vec<(String, bool)> = date_ident(style_vec)?;
        let name_resolve: Vec<String> = str_resolve(str_content)?;
        let c_list: Vec<String> = Vec::new();
        let tx1 = self.pro.lock().unwrap().clone();
        let tx_err = self.e_pro.lock().unwrap().clone();

        let closure = move || -> PyResult<()> {
            let xml_reader = Reader::from_reader(content.as_ref());
            let width_len = common_xml_length(xml_reader, tx_err.clone())?;
            let xml_reader: Reader<&[u8]> = Reader::from_reader(content.as_ref());
            let c_list = common_xml_handler(xml_reader, tx1.clone(), tx_err.clone(),
                                            &style_resolve, &name_resolve, c_list, &width_len, &chunk)?;
            let last_data = c_list.join("\n");
            let last_msg = String::from("finish");
            for v in vec![last_data, last_msg].into_iter(){
                if let Err(e) = tx1.send(v) {
                    let msg = format!("failed to send message: {}", e);
                    let _ = tx_err.send(msg);
                };
            }
            Ok(())
        };
        thread::spawn(closure);
        Ok(())
    }

    fn generate_data_chunk(&mut self) -> PyResult<String> {
        if let Ok(err_msg) = self.e_con.lock().unwrap().try_recv() {
            return Err(PyValueError::new_err(err_msg));
        }
        let e_msg = "failed to recv message";
        let data = self.con.lock().unwrap().recv().
            map_err(|e| PyValueError::new_err(format!("{}: {}", e_msg, e)))?;
        Ok(data)
    }
}

fn common_xml_handler(mut xml_reader: Reader<&[u8]>,
                      tx1: Sender<String>, tx_err: Sender<String>,
                      style_resolve: &[(String, bool)], name_resolve: &[String],
                      mut c_list: Vec<String>, width_len: &Option<usize>, chunk: &u32)
                      -> PyResult<Vec<String>> {

    let mut buffer: Vec<u8> = Vec::new();
    let mut row_a: Vec<Option<StructCsv>> = vec![None; width_len.unwrap()];
    let mut s: Option<StructCsv> = None;
    let mut is_v = false;
    let navi = create_navi();
    let v = loop {
        match xml_reader.read_event_into(&mut buffer) {
            Ok(Event::Start(e)) => {
                match e.name().as_ref() {
                    b"c" => {
                        let mut struct_csv = StructCsv::new();
                        for x in e.attributes().flatten() {
                            match x.key.into_inner() {
                                b"s" => {
                                    struct_csv.set_s_attr(
                                        x.key.into_inner().to_vec());
                                    let a = String::from_utf8(x.value.to_vec());
                                    if a.is_err() {
                                        let msg = "unable to parse utf8 string".to_string();
                                        let _ = tx_err.send(msg);
                                    }
                                    let b = a?.parse::<usize>();
                                    if b.is_err() {
                                        let msg = "unable to parse usize".to_string();
                                        let _ = tx_err.send(msg);
                                    }
                                    struct_csv.set_s_attr_v(b.unwrap());
                                }
                                b"t" => {
                                    struct_csv.set_t_attr(
                                        x.key.into_inner().to_vec());
                                    struct_csv.set_t_attr_v(
                                        x.clone().value.to_vec());
                                }
                                b"r" => {
                                    let a = String::from_utf8_lossy(
                                        &x.clone().value).
                                        into_owned();
                                    let b = column_to_number(a)?;
                                    struct_csv.set_r_attr_v(b - 1);
                                }
                                _ => {}
                            }
                            s = Some(struct_csv.clone());
                        }
                    },
                    b"v" => is_v = true,
                    _ => {},
                }
            }
            Ok(Event::End(e)) => {
                if let b"row" = e.name().as_ref() {
                    let i = row_a.into_iter().map(|a| {
                        match a {
                            Some(s) => {
                                let res: PyResult<String> = s.clone().
                                    get_value(&navi, name_resolve,
                                              style_resolve, tx_err.clone());
                                if let Err(err) = &res {
                                    let msg = format!("unable to parse structual: {}", err);
                                    let _ = tx_err.send(msg);
                                };
                                res
                            },
                            None => Ok("".to_string())
                        }
                    }).collect::<PyResult<Vec<String>>>();
                    let j = i?.join(",");
                    c_list.push(j);
                    if c_list.len() == (*chunk).try_into().unwrap() {
                        let val = c_list.join("\n");
                        if let Err(e) = tx1.send(val) {
                            let msg = format!("failed to send message: {}", e);
                            let _ = tx_err.send(msg);
                        };
                        c_list = Vec::new();
                    }
                    row_a = vec![None; width_len.unwrap()];
                }
            }
            Ok(Event::Text(e)) => {
                if is_v {
                    if let Some(ref mut v) = s {
                        let val = common_match_fn(e)?;
                        v.set_value(val);
                        let i = v.get_r_attr_v();
                        row_a[i] = Some(v.clone());
                        s = None;
                    }
                    is_v = false;
                }
            }
            Ok(Event::Eof) => {
                break c_list;
            }
            Err(e) => {
                let msg = format!("something wrong: {}", e);
                let _ = tx_err.send(msg);
            }
            _ => {}
        }
        buffer.clear();
    };
    Ok(v)
}

fn common_xml_length(mut xml_reader: Reader<&[u8]>, tx_err: Sender<String>)
    -> PyResult<Option<usize>> {

    let mut buffer: Vec<u8> = Vec::new();
    let idx = loop {
        match xml_reader.read_event_into(&mut buffer) {

            Err(e) => {
                let msg = format!("something wrong: {}", e);
                let _ = tx_err.send(msg);
            }
            _ => {
                // tag "dimension ref=\"xx:yy\"/" の属性取得
                let dimension_tag = b"dimension";
                if buffer.starts_with(dimension_tag){
                    let dim_tag = String::from_utf8_lossy(&buffer).into_owned();
                    let dim_tag_contains_colon = dim_tag.contains(':');
                    let dim_tag_last = if dim_tag_contains_colon {
                        dim_tag.split(':').last().unwrap()
                    } else {
                        let msg = "wrong dimension tag";
                        let _ = tx_err.send(msg.to_string().clone());
                        msg
                    };
                    let idx_num = column_to_number(dim_tag_last.to_string())?;
                    break idx_num;
                }
            }
        }
        buffer.clear();
    };
    Ok(Some(idx))
}

fn find_vec_index_rev<T: PartialEq>(vector: &[T], sub_vec: &[T]) -> Option<usize> {
    let s_vec_len = sub_vec.len();

    for i in (s_vec_len..=vector.len()).rev() {
        if i >= s_vec_len && &vector[(i - s_vec_len)..i] == sub_vec {
            return Some(i - s_vec_len)
        }
    }
    None
}

fn date_ident(style_resolve: (Vec<String>, HashMap<String, String>))
    -> PyResult<Vec<(String, bool)>> {
    // style.xml cellXfsタグから情報を取得し日付判定mapを作成する
    let style_vec = style_resolve.0;
    let style_map = style_resolve.1;

    let resolve_map: PyResult<Vec<(String, bool)>> =
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
                let bool_ = matches!(
                    style_map.get(&num_fmt_str),
                    Some(s) if s.matches('y').count() >= 2 && s.matches('m').count() >= 1);
                bool_
            },
        };
        Ok((num_fmt_str, bool_value))
    }).collect();
    resolve_map
}

fn stle_resolve(content: Vec<u8>) ->  PyResult<(Vec<String>, HashMap<String, String>)> {
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
                      -> PyResult<String> {
    let num_fmt_id_start = target_tag.find(target_attr).
        ok_or(PyValueError::new_err(error_msg.to_string()))? + target_attr.len();
    let num_fmt_id_end = target_tag[num_fmt_id_start..].find(target_end).
        ok_or(PyValueError::new_err(error_msg.to_string()))? + num_fmt_id_start;
    Ok(target_tag[num_fmt_id_start..num_fmt_id_end].to_string())
}

fn str_resolve(content: Vec<u8>) ->  PyResult<Vec<String>> {
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
                if is_text & !&no_text_ {
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

fn common_match_fn(e: BytesText) -> PyResult<String> {
    let val = match e.unescape() {
        Ok(v) => {v.into_owned()}
        Err(err) => {
            let msg = format!("wrong BytesText: {}", err);
            return Err(PyValueError::new_err(msg))
        }
    };
    Ok(val)
}

fn column_to_number(s: String) -> PyResult<usize> {
    let e_msg = "Error: Unable to convert column to number";
    let column_index= s.to_uppercase().chars().
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
