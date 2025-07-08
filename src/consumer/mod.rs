use super::config::{ConsumerBenchmark, ConsumerScenario};
use super::units::{Bytes, Messages, Seconds};

use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::Message;

use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::u64;

use postgres::{Client, NoTls}; // Убедитесь, что postgres добавлен в Cargo.toml

struct ConsumerBenchmarkStats {
    messages: Messages,
    bytes: Bytes,
    time: Seconds,
}

impl ConsumerBenchmarkStats {
    fn new(messages: u64, bytes: u64, time: Duration) -> ConsumerBenchmarkStats {
        ConsumerBenchmarkStats {
            messages: Messages::from(messages),
            bytes: Bytes::from(bytes),
            time: Seconds(time),
        }
    }

    fn print(&self) {
        println!("Received: {}, {}", self.messages, self.bytes);
        println!("Elapsed:  {} ({}, {})", self.time, self.messages / self.time, self.bytes / self.time)
    }
}

fn get_topic_partitions_count<X: ConsumerContext, C: Consumer<X>>(consumer: &C, topic_name: &str) -> Option<usize> {
    let metadata = consumer.fetch_metadata(Some(topic_name), Duration::from_secs(30))
        .expect("Failed to fetch metadata");

    if metadata.topics().is_empty() {
        None
    } else {
        let partitions = metadata.topics()[0].partitions();
        if partitions.is_empty() {
            None  // Topic was auto-created
        } else {
            Some(partitions.len())
        }
    }
}

fn initialize_consumer<T: FromClientConfig + Consumer>(scenario: &ConsumerScenario) -> T {
    let consumer: T = scenario.client_config()
        .set("enable.partition.eof", "true")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[&scenario.topic])
        .expect("Can't subscribe to specified topics");
    consumer
}

fn run_base_consumer_benchmark(scenario: &ConsumerScenario) -> ConsumerBenchmarkStats {
    let consumer: BaseConsumer = initialize_consumer(scenario);
    let mut partition_eof = HashSet::new();
    let partition_count = get_topic_partitions_count(&consumer, &scenario.topic)
        .expect("Topic not found");

    let limit = if scenario.message_limit < 0 {
        u64::MAX
    } else {
        scenario.message_limit as u64
    };

    let expected_messages = limit;

    let mut client = Client::connect("host=postgres user=benchmark password=benchmark dbname=benchmark", NoTls).expect("Failed to connect to Postgres");
    // Очищаем таблицу перед началом теста
    client.execute("DELETE FROM messages", &[]).expect("Failed to clear messages table");
    let mut first_sent_timestamp: Option<u128> = None;
    let mut last_received_time: Option<u128> = None;
    let mut first_write_time: Option<u128> = None;
    let mut last_write_time: Option<u128> = None;
    let mut start_time = Instant::now();
    let mut messages = 0;
    let mut bytes = 0;

    while messages < limit {
        match consumer.poll(Duration::from_secs(1)) {
            None => {},
            Some(Ok(message)) => {
                if messages == 0 {
                    println!("First message received");
                    start_time = Instant::now();
                }
                let payload = message.payload().unwrap_or(&[]);
                if let Some(pos) = payload.iter().position(|&b| b == b':') {
                    let sent_ts = std::str::from_utf8(&payload[..pos]).unwrap().parse::<u128>().unwrap();
                    let actual_payload = &payload[pos+1..];
                    if messages == 0 {
                        first_sent_timestamp = Some(sent_ts);
                    }
                    let received_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                    // --- Изменения для first_write_time и last_write_time ---
                    let write_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                    if first_write_time.is_none() {
                        first_write_time = Some(write_time);
                    }
                    last_write_time = Some(write_time);
                    client.execute(
                        "INSERT INTO messages (sent_timestamp, received_timestamp, payload) VALUES ($1, $2, $3)",
                        &[&(sent_ts as i64), &(received_ts as i64), &actual_payload],
                    ).unwrap();
                    // Если это первая запись в БД, ничего не выводим
                    // Если это последняя запись в БД, выведем метрики
                    if messages + 1 == limit {
                        last_received_time = Some(received_ts);
                        // Метрика: Kafka transit time
                        if let (Some(start), Some(received)) = (first_sent_timestamp, last_received_time) {
                            let kafka_time = received - start;
                            println!("Kafka transit time for {} messages: {:.3} seconds", expected_messages, kafka_time as f64 / 1000.0);
                        }
                    }
                }
                messages += 1;
                bytes += message.payload_len() + message.key_len();
                if messages == limit {
                    break;
                }
            },
            Some(Err(KafkaError::PartitionEOF(p))) => {
                partition_eof.insert(p);
                if partition_eof.len() >= partition_count {
                    break
                }
            },
            Some(Err(error)) => {
                println!("Error {:?}", error);
            }
        }
    }

    // Определяем, сколько сообщений ожидалось по конфигу
    let expected_messages = if scenario.message_limit < 0 {
        messages
    } else {
        scenario.message_limit as u64
    };

    if messages == limit {
        client.query_one("SELECT COUNT(*) FROM messages", &[]).unwrap();
        last_write_time = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
        println!("Successfully inserted {} messages into the database", expected_messages);
    }

    // После завершения всех вставок (после while)
    if let (Some(first_db), Some(last_db)) = (first_write_time, last_write_time) {
        let db_time = last_db - first_db;
        println!("DB write time for {} messages: {:.3} seconds", expected_messages, db_time as f64 / 1000.0);
    }
    if let (Some(start), Some(last_db)) = (first_sent_timestamp, last_write_time) {
        let full_cycle = last_db - start;
        println!("Full cycle for {} messages: {:.3} seconds", expected_messages, full_cycle as f64 / 1000.0);
    }
    // --- Проверка количества записей в базе ---
    let row = client.query_one("SELECT COUNT(*) FROM messages", &[]).expect("Failed to count messages");
    let count: i64 = row.get(0);
    println!("Messages in DB: {}", count);
    if count != expected_messages as i64 {
        println!("WARNING: Expected {} messages, but found {} in DB!", expected_messages, count);
    }

    ConsumerBenchmarkStats::new(messages, bytes as u64, start_time.elapsed())
}

pub fn run(config: &ConsumerBenchmark, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    let stats = run_base_consumer_benchmark(scenario);

    stats.print();
}
