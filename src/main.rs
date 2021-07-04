use clap::clap_app;
use serde::Serialize;
use serde_json::json;
use std::io::{self, BufRead, BufReader};
use std::{fs::File, str::FromStr};

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
        (@arg url: -u --url +takes_value +required)
        (@arg key: -k --key +takes_value +required)
        (@arg batch: -b --batch +takes_value default_value("200"))
    )
    .get_matches();

    let stdin = io::stdin();

    let src: Box<dyn BufRead> = match matches.value_of("src") {
        Some(p) => match File::open(p) {
            Ok(f) => Box::new(BufReader::new(f)),
            Err(_) => todo!(),
        },
        None => Box::new(BufReader::new(stdin.lock())),
    };

    let client = Typesense::new(
        matches.value_of("url").unwrap(),
        matches.value_of("key").unwrap(),
    );

    client.create_collection();

    let mut buffer = Vec::with_capacity(matches.value_of_t("batch").unwrap());
    let mut count = 0;

    for line in src.lines() {
        let csv = line.expect("Failed to read line from input");

        if let Some(rec) = read_record(&csv) {
            buffer.push(rec);

            if buffer.len() == buffer.capacity() {
                count += buffer.len();
                println!("Importing {} lines, total {}", buffer.len(), count);
                client.import(&buffer);
                buffer.clear();
            }
        };
    }

    if buffer.len() > 0 {
        count += buffer.len();
        println!("Importing {} lines, total {}", buffer.len(), count);
        client.import(&buffer);
    }
}

fn read_record(csv: &str) -> Option<TaxRecord> {
    fn read_column<T: FromStr>(column: Option<&&str>) -> Option<T> {
        match column {
            Some(s) => match s.trim().parse::<T>() {
                Ok(v) => Some(v),
                Err(_) => None,
            },
            None => None,
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

struct Typesense {
    base_url: String,
    api_key: String,
}

impl Typesense {
    pub fn new<T>(base_url: T, api_key: T) -> Self
    where
        T: Into<String>,
    {
        Typesense {
            base_url: base_url.into(),
            api_key: api_key.into(),
        }
    }

    pub fn create_collection(&self) {
        let body = json!({
            "name": "records",
            "fields": [
                {"name": "cvr",            "type": "int32"                },
                {"name": "se",             "type": "int32"                },
                {"name": "company_name",   "type": "string"               },
                {"name": "company_type",   "type": "string", "facet": true},
                {"name": "year",           "type": "int32",  "facet": true},
                {"name": "taxable_income", "type": "int64"                },
                {"name": "deficit",        "type": "int64"                },
                {"name": "corporate_tax",  "type": "int64"                }
            ],
            "default_sorting_field": "cvr"
        })
        .to_string();

        self.new_req(minreq::Method::Post, "collections")
            .with_body(body.as_bytes())
            .send()
            .expect("Failed to create collection");
    }

    pub fn import(&self, records: &[TaxRecord]) {
        let mut lines = String::new();

        for r in records {
            if let Ok(json) = serde_json::to_string(&r) {
                lines.push_str(&json);
                lines.push('\n');
            }
        }

        self.new_req(
            minreq::Method::Post,
            "/collections/records/documents/import?action=upsert",
        )
        .with_body(lines.as_bytes())
        .send()
        .expect("Failed to import records");
    }

    fn new_req(&self, method: minreq::Method, endpoint: &str) -> minreq::Request {
        minreq::Request::new(method, format!("{}/{}", self.base_url, endpoint))
            .with_header("X-TYPESENSE-API-KEY", &self.api_key)
    }
}
