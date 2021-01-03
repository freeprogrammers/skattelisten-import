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

    let dst: Box<dyn Write> = match matches.value_of("dst") {
        Some(s) => match File::create(s) {
            Ok(f) => Box::new(BufWriter::new(f)),
            Err(_) => todo!()
        }
        None => Box::new(BufWriter::new(stdout.lock()))
    };

    fn read_field<T: FromStr>(field: Option<&&str>) -> Option<T> {
        match field {
            Some(s) => match s.trim().parse::<T>() {
                Ok(v) => Some(v),
                Err(_) => None
            },
            None => None
        }
    }
    
    let mut ser = serde_json::Serializer::new(dst);
    let mut seq = ser.serialize_seq(None).expect("Failed to start serialization sequence");

    for line in src.lines() {
        match line {
            Ok(csv) => {
                let columns: Vec<_> = csv.split(',').collect();

                let rec = TaxRecord {
                    cvr: match read_field(columns.get(CVR)) {
                        Some(v) => v,
                        None => continue
                    },
                    se: match read_field(columns.get(SE)) {
                        Some(v) => v,
                        None => continue
                    },
                    company_name: match columns.get(COMPANY_NAME) {
                        Some(v) => String::from(*v),
                        None => continue
                    },
                    company_type: match columns.get(COMPANY_TYPE) {
                        Some(v) => String::from(*v),
                        None => continue
                    },
                    year: match read_field(columns.get(YEAR)) {
                        Some(v) => v,
                        None => continue
                    },
                    taxable_income: read_field(columns.get(TAXABLE_INCOME)),
                    deficit: read_field(columns.get(DEFICIT)),
                    corporate_tax: read_field(columns.get(CORPORATE_TAX)),
                };

                seq.serialize_element(&rec).expect("Serializing record failed")
            },
            Err(_) => todo!()
        }
    }
}
