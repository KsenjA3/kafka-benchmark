use rdkafka::ClientContext;
use rdkafka::producer::{BaseProducer, DeliveryResult, ProducerContext, Producer, BaseRecord};

use std::cmp;
use std::iter::{IntoIterator, Iterator};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod content;

use self::content::CachedMessages;
use super::config::{ProducerBenchmark, ProducerType, ProducerScenario};
use super::units::{Bytes, Messages, Seconds};

struct BenchmarkProducerContext {
    failure_counter: Arc<AtomicUsize>,
}

impl BenchmarkProducerContext {
    fn new() -> BenchmarkProducerContext {
        BenchmarkProducerContext {
            failure_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ClientContext for BenchmarkProducerContext {}

impl ProducerContext for BenchmarkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, r: &DeliveryResult, _: Self::DeliveryOpaque) {
        if r.is_err() {
            self.failure_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_thread(
    thread_id: u64,
    scenario: &ProducerScenario,
    cache: &CachedMessages,
) -> ThreadStats {
    let producer: BaseProducer<_> = scenario.client_config()
        .create().expect("Producer creation failed");
    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    let mut failures = 0;
    for (count, content) in cache.into_iter().take(per_thread_messages as usize).enumerate() {
        let payload = scenario_name_timestamp_payload(scenario, content);
        let partition = (count as i32 % 24) as i32;
        let record = BaseRecord::to(&scenario.topic)
            .payload(&payload)
            .key(&[])
            .partition(partition);
        if let Err((e, _)) = producer.send(record) {
            println!("Kafka error: {:?}", e);
            failures += 1;
        }
    }
    producer.flush(Duration::from_secs(10));
    ThreadStats::new(start.elapsed(), failures)
}



fn scenario_name_timestamp_payload(_scenario: &ProducerScenario, content: &[u8]) -> Vec<u8> {
    let start_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let mut payload = start_timestamp.to_string().into_bytes();
    payload.push(b':');
    payload.extend_from_slice(content);
    payload
}

pub fn run(config: &ProducerBenchmark, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!(
        "Scenario: {}, repeat {} times, {} seconds pause after each",
        scenario_name, scenario.repeat_times, scenario.repeat_pause
    );

    let mut benchmark_stats = ProducerBenchmarkStats::new(scenario);
    for i in 0..scenario.repeat_times {
        let mut scenario_stats = ProducerRunStats::new(scenario);
        let threads = (0..scenario.threads)
            .map(|thread_id| {
                let scenario = scenario.clone();
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    base_producer_thread(thread_id, &scenario, &cache)
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            let stats = thread.join();
            scenario_stats.merge_thread_stats(&stats.unwrap());
        }
        scenario_stats.print();
        benchmark_stats.add_stat(scenario_stats);
        if i != scenario.repeat_times - 1 {
            thread::sleep(Duration::from_secs(scenario.repeat_pause as u64))
        }
    }
    benchmark_stats.print();
}


#[derive(Debug)]
pub struct ThreadStats {
    duration: Duration,
    failure_count: usize,
}

impl ThreadStats {
    pub fn new(duration: Duration, failure_count: usize) -> ThreadStats {
        ThreadStats {
            duration,
            failure_count,
        }
    }
}

#[derive(Debug)]
pub struct ProducerRunStats<'a> {
    scenario: &'a ProducerScenario,
    failure_count: usize,
    duration: Duration,
}

impl<'a> ProducerRunStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerRunStats<'a> {
        ProducerRunStats {
            scenario,
            failure_count: 0,
            duration: Duration::from_secs(0),
        }
    }

    pub fn merge_thread_stats(&mut self, thread_stats: &ThreadStats) {
        self.failure_count += thread_stats.failure_count;
        self.duration = cmp::max(self.duration, thread_stats.duration);
    }

    pub fn print(&self) {
        let time = Seconds(self.duration);
        let expected_messages = self.scenario.message_count;
        let messages = Messages::from(expected_messages);
        let bytes = Bytes::from(expected_messages * self.scenario.message_size);

        if self.failure_count != 0 {
            println!(
                "Warning: {} messages failed to be delivered",
                self.failure_count
            );
        }

        println!(
            "* Produced {} ({}) in {} using {} thread{}\n    {}\n    {}",
            expected_messages,
            bytes,
            time,
            self.scenario.threads,
            if self.scenario.threads > 1 { "s" } else { "" },
            messages / time,
            bytes / time
        );
    }
}

pub struct ProducerBenchmarkStats<'a> {
    scenario: &'a ProducerScenario,
    stats: Vec<ProducerRunStats<'a>>,
}

impl<'a> ProducerBenchmarkStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerBenchmarkStats<'a> {
        ProducerBenchmarkStats {
            scenario,
            stats: Vec::new(),
        }
    }

    pub fn add_stat(&mut self, scenario_stat: ProducerRunStats<'a>) {
        self.stats.push(scenario_stat)
    }

    pub fn print(&self) {
        let time = Seconds(self.stats.iter().map(|stat| stat.duration).sum());
        let expected_messages = self.scenario.message_count * self.stats.len() as u64;
        let messages = Messages::from(expected_messages);
        let bytes = Bytes(messages.0 * self.scenario.message_size as f64);

        println!("Average: {}, {}", messages / time, bytes / time);
    }
}
