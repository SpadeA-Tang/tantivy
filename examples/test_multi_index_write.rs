use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::BufRead;
use std::sync::Arc;
use std::thread::{self};
use std::time::{Duration, Instant};

use rand::Rng;
use tantivy::indexer::IndexWriterOptions;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter};
use tokio::runtime::Builder;

fn random_text(len: usize, word_len: usize) -> String {
    let mut s = String::new();
    for _ in 0..len {
        s.push_str(&random_string(word_len));
        s.push(' ');
    }
    s
}

fn random_string(len: usize) -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    String::from_utf8(thread_rng().sample_iter(&Alphanumeric).take(len).collect()).unwrap()
}

lazy_static::lazy_static! {
    static ref RandomTexts: Vec<String> = {
        let mut texts = vec![];
        for _ in 0..100 {
            texts.push(random_text(20, 3));
        }
        texts.into_iter().collect::<Vec<String>>()
    };
}

pub fn thread_ids(pid: u32) -> io::Result<Vec<u32>> {
    let dir = fs::read_dir(format!("/proc/{}/task", pid))?;
    Ok(dir
        .filter_map(|task| {
            let file_name = match task {
                Ok(t) => t.file_name(),
                Err(_) => {
                    return None;
                }
            };

            match file_name.to_str() {
                Some(tid) => match tid.parse() {
                    Ok(tid) => Some(tid),
                    Err(_) => None,
                },
                None => None,
            }
        })
        .collect::<Vec<u32>>())
}

fn get_thread_switch_info(pid: u32, tid: u32) -> io::Result<(u64, u64)> {
    let path = format!("/proc/{}/task/{}/status", pid, tid);
    let file = fs::File::open(&path)?;
    let reader = io::BufReader::new(file);
    let mut voluntary = None;
    let mut nonvoluntary = None;

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("voluntary_ctxt_switches:") {
            voluntary = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok());
        } else if line.starts_with("nonvoluntary_ctxt_switches:") {
            nonvoluntary = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok());
        }
    }
    match (voluntary, nonvoluntary) {
        (Some(vol), Some(nonvol)) => Ok((vol, nonvol)),
        _ => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "无法获取上下文切换信息",
        )),
    }
}

// document_count  commit_count  duration
// 1000            2             5.1s
// 100             10            18s
fn main() {
    // let args: Vec<String> = std::env::args().collect();
    // let run_seconds: u64 = args[1].parse().unwrap();
    let runtime = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap(),
    );

    let t = Instant::now();
    let n = 20;
    let document_count = 10000;
    let commit_count = 2;
    let mut hanlders = vec![];
    let num_threads = 16;
    let options = IndexWriterOptions::builder()
        .num_worker_threads(1)
        .memory_budget_per_thread(50_000_000)
        .runtime(runtime)
        .build();
    for _ in 0..num_threads {
        let options_clone = options.clone();
        hanlders.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut indexs = vec![];
            let mut index_writers = vec![];
            for _ in 0..n {
                let mut schema_builder = Schema::builder();
                schema_builder.add_text_field("body", TEXT);
                let schema = schema_builder.build();

                let index = Index::create_in_ram(schema.clone());
                let body = schema.get_field("body").unwrap();

                let index_writer: IndexWriter =
                    index.writer_with_options(options_clone.clone()).unwrap();
                indexs.push(index);
                index_writers.push((index_writer, body));
            }

            for _ in 0..commit_count {
                for _ in 0..document_count {
                    for (index_writer, body) in &index_writers {
                        let random_idx = rng.gen_range(0..RandomTexts.len());
                        let body_str = RandomTexts[random_idx].clone();
                        index_writer
                            .add_document(doc!(
                                *body => body_str
                            ))
                            .unwrap();
                    }
                }
                for (index_writer, _) in &mut index_writers {
                    index_writer.commit().unwrap();
                }
            }
        }));
    }

    let (tx, rx) = oneshot::channel();
    let collector = thread::spawn(move || -> HashMap<u32, (u64, u64)> {
        let process_id = std::process::id();
        let mut metrics = HashMap::new();
        loop {
            if rx.try_recv().is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
            let tids = thread_ids(process_id).unwrap();
            for tid in tids {
                if let Ok((voluntary_ctxt_switches, nonvoluntary_ctxt_switches)) =
                    get_thread_switch_info(process_id, tid)
                {
                    metrics.insert(tid, (voluntary_ctxt_switches, nonvoluntary_ctxt_switches));
                }
            }
        }
        metrics
    });

    let mut acc = 0;
    for h in hanlders {
        h.join().unwrap();
        acc += 1;
        if acc == num_threads / 2 {
            println!("time spend, {:?}", t.elapsed());
        }
    }
    println!("time spend, {:?}", t.elapsed());
    tx.send(()).unwrap();

    let mut voluntary_ctxt_switches = 0;
    let mut nonvoluntary_ctxt_switches = 0;
    let mut collected = 0;
    for (_, (v, non)) in collector.join().unwrap() {
        voluntary_ctxt_switches += v;
        nonvoluntary_ctxt_switches += non;
        collected += 1;
    }

    println!(
        "voluntary_ctxt_switches: {}, nonvoluntary_ctxt_switches: {}, collected {}",
        voluntary_ctxt_switches, nonvoluntary_ctxt_switches, collected
    );
}
