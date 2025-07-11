#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate rand;
extern crate rdkafka;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate postgres;

mod config;
mod consumer;
mod producer;
mod units;

use config::{ConsumerBenchmark, ProducerBenchmark};
use std::thread;
use std::time::Duration;

fn main() {
    let matches = clap_app!(app =>
        (name: "kafka benchmark")
        (@arg benchmark_type: +takes_value +required "Benchmark type ('producer' or 'consumer')")
        (@arg config: +takes_value +required "The configuration file")
        (@arg scenario: +takes_value +required "The scenario you want to execute")
    ).get_matches();

    env_logger::init();

    let config_file = matches.value_of("config").unwrap();
    let scenario_name = matches.value_of("scenario").unwrap();

    // Добавили небольшую задержку, чтобы дать Kafka и инициализатору топиков время
    println!("Waiting 10 seconds for Kafka and topics to be ready...");
    thread::sleep(Duration::from_secs(10));

    match matches.value_of("benchmark_type").unwrap() {
        "consumer" => consumer::run(&ConsumerBenchmark::from_file(config_file), scenario_name),
        "producer" => producer::run(&ProducerBenchmark::from_file(config_file), scenario_name),
        _ => println!("Undefined benchmark type. Please use 'producer' or 'consumer'"),
    }
}
