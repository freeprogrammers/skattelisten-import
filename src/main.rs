use clap::clap_app;
use serde::Serialize;
use serde_json::json;
use std::io::{self, BufRead, BufReader};
use std::{fs::File, str::FromStr};

const CVR: usize = 0;
const NAME: usize = 1;

#[derive(Debug, Serialize)]
struct Company {
    cvr: u32,
    name: String,
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

        if let Some(company) = read_company(&csv) {
            buffer.push(company);

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

fn read_company(csv: &str) -> Option<Company> {
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

    Some(Company {
        cvr: read_column(columns.get(CVR))?,
        name: read_column(columns.get(NAME))?,
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
            "name": "companies",
            "fields": [
                {"name": "cvr",  "type": "int32" },
                {"name": "name", "type": "string"},
            ],
            "default_sorting_field": "cvr"
        })
        .to_string();

        self.new_req(minreq::Method::Post, "collections")
            .with_body(body.as_bytes())
            .send()
            .expect("Failed to create collection");
    }

    pub fn import(&self, companies: &[Company]) {
        let mut lines = String::new();

        for r in companies {
            if let Ok(json) = serde_json::to_string(&r) {
                lines.push_str(&json);
                lines.push('\n');
            }
        }

        self.new_req(
            minreq::Method::Post,
            "/collections/companies/documents/import?action=upsert",
        )
        .with_body(lines.as_bytes())
        .send()
        .expect("Failed to import companies");
    }

    fn new_req(&self, method: minreq::Method, endpoint: &str) -> minreq::Request {
        minreq::Request::new(method, format!("{}/{}", self.base_url, endpoint))
            .with_header("X-TYPESENSE-API-KEY", &self.api_key)
    }
}
