use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use crate::{DBImpl, DB};
use crate::engine::background::FlushMemTableCommand;
use crate::engine::background::task::Command;
use crate::engine::mem::{MemTable, SkipListMemTable};
use crate::engine::sst::table_builder::TableBuilder;


struct Inner {
    queue: Mutex<VecDeque<Box<dyn Command>>>,
    cv: Condvar,
    shutting_down: Mutex<bool>,
}

pub struct BackgroundWorker {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundWorker {
    pub fn new(db: Arc<dyn DB>) -> Self{
        Self::start()
    }
    
    pub fn start() -> Self {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutting_down: Mutex::new(false),
        });

        let worker_inner = Arc::clone(&inner);

        let handle = thread::spawn(move || {
            Self::background_loop(worker_inner);
        });

        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub fn schedule_task(&self, task: Box<dyn Command>) {
        let mut queue = self.inner.queue.lock()
            .unwrap_or_else(|e| e.into_inner());
        queue.push_back(task);
        self.inner.cv.notify_one();
    }

    pub fn schedule_flush(
        &self,
        db: &Arc<DBImpl>,
        imm: VecDeque<Arc<dyn MemTable>>,
    ) {
        let cmd: Box<dyn Command> = Box::new(FlushMemTableCommand::new(db, imm));
        self.schedule_task(cmd);
    }

    fn background_loop(inner: Arc<Inner>) {
        loop {
            let task_opt = {
                let mut queue = inner.queue.lock().unwrap();
                while queue.is_empty() {
                    if *inner.shutting_down.lock().unwrap() {
                        return;
                    }
                    queue = inner.cv.wait(queue).unwrap();
                }
                queue.pop_front()
            };

            if let Some(cmd) = task_opt {
                cmd.execute();
            }
        }
    }

    pub fn shutdown(&self) {
        {
            let mut shutting_down = self.inner.shutting_down.lock().unwrap();
            *shutting_down = true;
        }

        self.inner.cv.notify_all();

        if let Some(handle) = self.handle.as_ref() {
            handle.join().unwrap();
        }
    }
}


