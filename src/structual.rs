use chrono::{Duration, NaiveDateTime};
use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

#[derive( Debug, Clone)]
pub(crate) struct StructCsv {
    value: String,
    s_attr: u8,
    s_attr_v: i32,
    t_attr: u8,
    r_attr_v: usize
}

impl StructCsv {
    pub(crate) fn new () -> StructCsv {
        StructCsv{
            value: "".to_string(),
            s_attr: 0u8,
            s_attr_v: 0,
            t_attr: 0u8,
            r_attr_v: 0
        }
    }

    pub(crate) fn set_s_attr(&mut self, val: u8) {
        self.s_attr = val;
    }

    pub(crate) fn set_s_attr_v(&mut self, val: i32) {
        self.s_attr_v = val;
    }

    pub(crate) fn set_t_attr(&mut self, val: u8) {
        self.t_attr = val;
    }

    pub(crate) fn set_r_attr_v(&mut self, val: usize) {
        self.r_attr_v = val;
    }

    pub(crate) fn get_r_attr_v(&self) -> usize {
        self.r_attr_v.clone()
    }

    pub(crate) fn set_value(&mut self, val: String) {
        self.value = val;
    }

    pub(crate) fn get_value(&self, excel_base_date: &NaiveDateTime,
                            name_resolve: &Vec<String>) -> Result<String, PyErr> {

        if self.t_attr == 116u8 {
            let i: usize = match self.value.parse::<usize>() {
                Ok(i) => i,
                Err(e) => {
                    let msg = format!("unable to parse address value: {}", e);
                    return Err(PyValueError::new_err(msg))
                }
            };
            Ok(format!("\"{}\"", name_resolve[i].as_str().to_string()))
        } else if self.s_attr == 115u8 {
            if self.s_attr_v < 3 {
                let a = &self.value[..];
                Ok(self.excel_date_to_datetime(a, excel_base_date)?)
            } else {
                Ok(self.value.as_str().to_string())
            }
        } else {
            Ok(self.value.as_str().to_string())
        }
    }

    fn excel_date_to_datetime(&self, val: &str, excel_base_date: &NaiveDateTime)
        -> Result<String, PyErr> {
        // エクセルの数値を日数と秒に分割
        let day_msg = "unable to parse day value";
        let days_str = val.split('.').next().
            ok_or(PyValueError::new_err(day_msg))?;
        let days = days_str.parse().
            map_err(|e| PyValueError::new_err(format!("{}: {}", &day_msg, e)))?;

        let second_msg = "unable to parse seconds value";
        let parse_value = val.parse::<f64>().
            map_err(|e| PyValueError::new_err(format!("{}: {}", &second_msg, e)))?;
        let seconds: i64 = ((parse_value - days as f64) * 86400.5) as i64;

        // エクセルの基準日に指定された秒数を加算
        let result = *excel_base_date +
            Duration::days(days) +
            Duration::seconds(seconds);

        // 時刻の部分を取得し、文字列としてフォーマット
        let time_format = result.format(if val.contains('.') {
            "%Y-%m-%d %H:%M:%S"
        } else {
            "%Y-%m-%d"
        }).to_string();
        Ok(time_format)
    }
}