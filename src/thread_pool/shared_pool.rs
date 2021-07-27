use std::thread;
use std::thread::{Builder, JoinHandle};

use anyhow::Result;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use crossbeam::channel::unbounded;
use log::error;
use log::warn;

use crate::thread_pool::ThreadPool;

type TaskClosure = Box<dyn FnOnce() + Send + 'static>;

enum TaskMessage {
    NewTask(TaskClosure),
    Shutdown,
}

struct WorkerGuard(Receiver<TaskMessage>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if thread::panicking() {
            let name = thread::current().name().unwrap().to_string();
            warn!("Thread: {} panics, start a reliever.", name);
            let rx = self.0.clone();
            Builder::new()
                .name(name)
                .spawn(move || thread_main_loop(WorkerGuard(rx)))
                .unwrap();
        }
    }
}

/// A simple thread pool implement by channel.
pub struct SharedQueueThreadPool {
    tx: Sender<TaskMessage>,
    threads: Vec<JoinHandle<()>>,
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads.len() {
            self.tx.send(TaskMessage::Shutdown).unwrap();
        }
        // self.threads
        //     .into_iter()
        //     .for_each(
        //         |t| { t.join(); }
        //     );
    }
}

fn thread_main_loop(guard: WorkerGuard) {
    while let Ok(message) = guard.0.recv() {
        match message {
            TaskMessage::NewTask(task) => task(),
            TaskMessage::Shutdown => return,
        }
    }

    error!(
        "Thread: {} exists because channel is broken.",
        thread::current().name().unwrap_or("Unknown")
    );
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
        where
            Self: Sized,
    {
        let (tx, rx) = unbounded();
        let thread_handler: Vec<_> = (0..threads)
            .map(|idx| {
                let rx = rx.clone();
                Builder::new()
                    .name(format!("SharedQueueThreadPool-thread: {}", idx + 1))
                    .spawn(move || thread_main_loop(WorkerGuard(rx)))
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to spawn the working threads in thread pool");
        Ok(Self {
            tx,
            threads: thread_handler,
        })
    }

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static,
    {
        match self.tx.send(TaskMessage::NewTask(Box::new(job))) {
            Ok(_) => (),
            Err(e) => panic!("{:}", e.to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test() {
        use crossbeam::channel;
        use std::thread;
        let (tx, rx) = channel::unbounded::<String>();
        for _i in 0..4 {
            let _rx = rx.clone();
            thread::spawn(move || loop {
                sleep(Duration::from_millis(200))
            });
        }
        drop(rx);
        tx.send("1".to_owned()).unwrap();
        tx.send("2".to_owned()).unwrap();
        tx.send("3".to_owned()).unwrap();
        tx.send("4".to_owned()).unwrap();
        tx.send("5".to_owned()).unwrap();
        tx.send("6".to_owned()).unwrap();
        tx.send("7".to_owned()).unwrap();
        tx.send("8".to_owned()).unwrap();
        sleep(Duration::from_secs(5));
    }
}
