use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::engine::mem::Storage;

type SharedStorage = Arc<tokio::sync::Mutex<Storage>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:6379").await?;
    let storage = Arc::new(tokio::sync::Mutex::new(Storage::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            handle_connection(socket, storage_clone).await;
        });
    }
}

// 连接处理
async fn handle_connection(mut socket: TcpStream, storage: SharedStorage) {
    let mut buf = [0u8; 1024];
    loop {
        let n = match socket.read(&mut buf).await {
            Ok(0) => return, // connection closed
            Ok(n) => n,
            Err(_) => return,
        };

        // 简单 RESP parser demo (这里只是伪解析)
        let command = String::from_utf8_lossy(&buf[..n]);
        let response = process_command(command.to_string(), storage.clone()).await;

        let _ = socket.write_all(response.as_bytes()).await;
    }
}

async fn process_command(cmd: String, storage: SharedStorage) -> String {
    let tokens: Vec<&str> = cmd.trim().split_whitespace().collect();
    if tokens.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }

    match tokens[0].to_uppercase().as_str() {
        "PING" => "+PONG\r\n".to_string(),
        "SET" => {
            if tokens.len() < 3 { return "-ERR SET needs key value\r\n".to_string(); }
            let key = tokens[1].to_string();
            let value = tokens[2].to_string();
            storage.lock().await.set(key, value);
            "+OK\r\n".to_string()
        },
        "GET" => {
            if tokens.len() < 2 { return "-ERR GET needs key\r\n".to_string(); }
            let key = tokens[1];
            match storage.lock().await.get(key) {
                Some(v) => format!("${}\r\n{}\r\n", v.len(), v),
                None => "$-1\r\n".to_string()
            }
        },
        _ => "-ERR unknown command\r\n".to_string()
    }
}
