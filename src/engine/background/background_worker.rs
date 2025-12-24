use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type Task = Box<dyn FnOnce() + Send + 'static>;

struct Inner {
    queue: Mutex<VecDeque<Task>>,
    cv: Condvar,
    shutting_down: Mutex<bool>,
}

pub struct BackgroundWorker {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundWorker {
    pub fn start() -> Self {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutting_down: Mutex::new(false),
        });

        let worker_inner = Arc::clone(&inner);

        let handle = thread::spawn(move || {
            background_loop(worker_inner);
        });

        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut queue = self.inner.queue.lock()
            .unwrap_or_else(|e| e.into_inner());();
        queue.push_back(Box::new(f));
        self.inner.cv.notify_one();
    }

    pub fn shutdown(mut self) {
        {
            let mut shutting_down = self.inner.shutting_down.lock().unwrap();
            *shutting_down = true;
        }

        self.inner.cv.notify_all();

        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

fn background_loop(inner: Arc<Inner>) {
    loop {
        let task = {
            let mut queue = inner.queue.lock()
                .unwrap_or_else(|e| e.into_inner());

            // 和 RocksDB 一样：用 while 防虚假唤醒
            while queue.is_empty() {
                if *inner.shutting_down.lock().unwrap() {
                    return;
                }
                queue = inner.cv.wait(queue).unwrap();
            }

            queue.pop_front()
        };

        if let Some(task) = task {
            task(); // 顺序执行
        }
    }
}