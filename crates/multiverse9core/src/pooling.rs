use log::*;
use std::sync::{mpsc, Arc, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    id: usize,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, rx: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = std::thread::spawn(move || loop {
            let rx = rx.lock().unwrap().recv();
            match rx {
                Ok(job) => job(),
                Err(_) => break,
            }
        });

        Self {
            id,
            thread: Some(thread),
        }
    }
}

pub struct Pool {
    workers: Vec<Worker>,
    tx: Option<mpsc::Sender<Job>>,
}

impl Drop for Pool {
    fn drop(&mut self) {
        drop(self.tx.take());

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                debug!("Shutting down worker {}", worker.id);
                thread.join().unwrap();
            }
        }
    }
}

impl Pool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&rx)));
        }

        Self {
            workers,
            tx: Some(tx),
        }
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) {
        let job = Box::new(f);
        self.tx.as_ref().unwrap().send(job).unwrap();
    }
}
