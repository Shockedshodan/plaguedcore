use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use crate::CensoredTransaction;

pub fn write_json(filename: &str, data: &Vec<CensoredTransaction>) -> std::io::Result<()> {
    let json_data = serde_json::to_string_pretty(&data)?;
    let mut file = File::create(filename)?;
    file.write_all(json_data.as_bytes())?;
    Ok(())
}

pub fn append_json(filename: &str, new_data: &CensoredTransaction) -> std::io::Result<()> {
    let mut file = File::open(filename)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let mut data: Vec<CensoredTransaction> = serde_json::from_str(&contents)?;
    data.push(new_data.clone());

    write_json(filename, &data)
}

pub fn file_exists(file_path: &str) -> bool {
    Path::new(file_path).exists()
}

pub fn deal_with_json(new_data: &CensoredTransaction, origin: &str) {
    let filename = format!("Censored_transactions_{}.json", origin);
    if file_exists(&filename) {
        append_json(&filename, new_data).unwrap();
    } else {
        write_json(&filename, &vec![new_data.clone()]).unwrap();
    }
}
