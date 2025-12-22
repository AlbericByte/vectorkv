use std::collections::HashMap;

pub struct Storage {
    db: HashMap<String, String>,
}

impl Storage {
    pub fn new() -> Self {
        Storage { db: HashMap::new() }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.db.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.db.get(key).cloned()
    }
}
