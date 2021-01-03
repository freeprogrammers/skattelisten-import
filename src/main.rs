use clap::clap_app;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use serde::ser::{SerializeSeq, Serializer};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};

#[derive(Debug, Deserialize, Serialize)]
struct TaxRecord {
    #[serde(rename = "CVR-nr.")]
    cvr: u32,
    #[serde(rename = "SE-nr.")]
    se: u32,
    #[serde(rename = "Navn")]
    company_name: String,
    #[serde(rename = "Selskabstype")]
    company_type: String,
    #[serde(rename = "Indkomstår")]
    year: u16,
    #[serde(rename = "Skattepligtig indkomst")]
    taxable_income: Option<i64>,
    #[serde(rename = "Underskud")]
    deficit: Option<i64>,
    #[serde(rename = "Selskabsskat")]
    corporate_tax: Option<i64>,
}

fn main() {
    let matches = clap_app!(slimp => 
        (@arg src: -s --source +takes_value)
        (@arg dst: -d --destination +takes_value)
    ).get_matches();

    let mut stderr = io::stderr();

    let src: Box<dyn Read> = match matches.value_of("src") {
        Some(s) => match File::open(s) {
            Ok(f) => Box::new(BufReader::new(f)),
            Err(_) => todo!()
        }
        None => Box::new(io::stdin())
    };

    let dst: Box<dyn Write> = match matches.value_of("dst") {
        Some(s) => match File::create(s) {
            Ok(f) => Box::new(BufWriter::new(f)),
            Err(_) => todo!()
        }
        None => Box::new(io::stdout())
    };

    let mut rdr = ReaderBuilder::new()
        .double_quote(false)
        .trim(csv::Trim::All)
        .from_reader(src);

    let mut ser = serde_json::Serializer::new(dst);
    let mut seq = ser.serialize_seq(None).expect("Opening serialization sequence failed");
    
    for res in rdr.deserialize::<TaxRecord>() {
        match res {
            Ok(rec) => seq.serialize_element(&rec).expect("Serializing record failed"),
            Err(e) => {
                let _ = writeln!(stderr, "Failed to read record: {}", e.to_string());
            }
        }
    }

    seq.end().expect("Failed to finish serialization");
}
