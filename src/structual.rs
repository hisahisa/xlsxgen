use chrono::{Duration, NaiveDateTime};

#[derive( Debug, Clone)]
pub(crate) struct StructCsv {
    value: String,
    attr: u8
}

impl StructCsv {
    pub(crate) fn new (attr: u8) -> StructCsv {
        StructCsv{
            value: "".to_string(),
            attr
        }
    }

    pub(crate) fn set_value(&mut self, val: String) {
        self.value = val;
    }

    pub(crate) fn get_value(&self, excel_base_date: &NaiveDateTime,
                            name_resolve: &Vec<String>) -> String{
        if self.attr == 115u8 {
            let a = &self.value[..];
            self.excel_date_to_datetime(a, excel_base_date)
        } else if self.attr == 116u8 {
            let i: usize = self.value.parse::<usize>().unwrap();
            format!("\"{}\"", name_resolve[i].clone())
        } else {
            self.value.clone()
        }
    }

    fn excel_date_to_datetime(&self, val: &str, excel_base_date: &NaiveDateTime)
        -> String {
        // エクセルの数値を日数と秒に分割
        let days: i64 = val.split('.').next().unwrap().parse().unwrap();
        let seconds: i64 = ((val.parse::<f64>().unwrap() - days as f64) * 86400.5) as i64;

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
        time_format
    }
}