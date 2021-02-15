use clap::clap_app;
use serde::{Serialize, Serializer};
use serde::ser::SerializeSeq;
use std::{fs::File, str::FromStr};
use std::io::{self, BufRead, BufReader, BufWriter, Write};

const CVR: usize = 0;
const COMPANY_NAME: usize = 1;
const SE: usize = 2;
const YEAR: usize = 3;
const COMPANY_TYPE: usize = 5;
const TAXABLE_INCOME: usize = 8;
const DEFICIT: usize = 9;
const CORPORATE_TAX: usize = 10;

#[derive(Debug, Serialize)]
struct TaxRecord {
    cvr: u32,
    se: u32,
    company_name: String,
    company_type: String,
    year: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    taxable_income: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deficit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    corporate_tax: Option<i64>,
}

fn main() {
    let matches = clap_app!(slimp =>
        (@arg src: -s --source +takes_value)
        (@arg dst: -d --destination +takes_value)
    ).get_matches();

    let stdout = io::stdout();
    let stdin = io::stdin();

    let src: Box<dyn BufRead> = match matches.value_of("src") {
        Some(p) => match File::open(p) {
            Ok(f) => Box::new(BufReader::new(f)),
            Err(_) => todo!()
        },
        None => Box::new(BufReader::new(stdin.lock()))
    };

    let mut dst: Box<dyn Write> = match matches.value_of("dst") {
        Some(s) => match File::create(s) {
            Ok(f) => Box::new(BufWriter::new(f)),
            Err(_) => todo!()
        }
        None => Box::new(BufWriter::new(stdout.lock()))
    };

    let mut ser = serde_json::Serializer::new(Vec::new());
    let mut seq = ser.serialize_seq(None).expect("Failed to start serialization sequence");

    for line in src.lines() {
        match line {
            Ok(csv) => {
                if let Some(rec) = read_record(&csv) {
                    seq.serialize_element(&rec).expect("Serializing record failed")
                };
            },
            Err(_) => todo!()
        }
    }

    seq.end().expect("Failed to end serialization sequence");

    dst.write_all(ser.into_inner().as_slice()).expect("Failed to write json to destination");
}

fn read_record(csv: &str) -> Option<TaxRecord> {
    fn read_column<T: FromStr>(column: Option<&&str>) -> Option<T> {
        match column {
            Some(s) => match s.trim().parse::<T>() {
                Ok(v) => Some(v),
                Err(_) => None
            },
            None => None
        }
    }

    let columns: Vec<_> = csv.split(',').collect();

    Some(TaxRecord {
        cvr: read_column(columns.get(CVR))?,
        se: read_column(columns.get(SE))?,
        company_name: read_column(columns.get(COMPANY_NAME))?,
        company_type: read_column(columns.get(COMPANY_TYPE))?,
        year: read_column(columns.get(YEAR))?,
        taxable_income: read_column(columns.get(TAXABLE_INCOME)),
        deficit: read_column(columns.get(DEFICIT)),
        corporate_tax: read_column(columns.get(CORPORATE_TAX)),
    })
}
