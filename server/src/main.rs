#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Сервер стриминга котировок
//! Пример запуска:
//! cargo run -- --path tickers.txt --port 8080

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

use clap::Parser;
use crossbeam::channel;

mod client_manager;
mod command_handler;
mod quote_generator;

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "tickers.txt")]
    path: String,

    #[clap(long, default_value = "8080")]
    port: u16,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .format_target(true)
        .init();

    // Обработка сигнала завершения
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("Получен сигнал завершения, освобождаем ресурсы...");
        r.store(false, Ordering::SeqCst);
    })?;
    let mut handles = vec![];

    let args = Args::parse();
    log::info!("Запуск сервера");

    let tickers = load_tickers(args.path).unwrap_or_else(|e| {
        log::error!("Ошибка загрузки файла: {}", e);
        let q = vec![
            "AAPL".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "AMZN".to_string(),
            "TSLA".to_string(),
        ];
        log::info!("Используются предустановленные: {:?}", q);
        q
    });

    log::info!("Загружено {} тикетов", tickers.len());

    let client_manager = Arc::new(Mutex::new(client_manager::ClientManager::new()));

    let (quote_sender, quote_receiver) = channel::unbounded();

    let running_clone = running.clone();
    let handler = start_quote_generator(tickers, quote_sender.clone(), running_clone);
    handles.push(handler);

    let running_clone = running.clone();
    let handler = start_inactive_client_monitor(client_manager.clone(), running_clone);
    handles.push(handler);

    let handler = start_tcp_server(client_manager, quote_receiver.clone(), args.port, running)?;
    handles.extend(handler);

    for handle in handles {
        handle.join().unwrap();
    }
    log::info!("Все дочерние потоки завершены!");
    Ok(())
}

fn load_tickers(path: String) -> Result<Vec<String>, std::io::Error> {
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn start_quote_generator(
    tickers: Vec<String>,
    quote_sender: channel::Sender<quote_lib::StockQuote>,
    running: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        log::info!("Запуск потока генерации котировок");
        let mut generator = quote_generator::QuoteGenerator::new(tickers);
        while running.load(Ordering::SeqCst) {
            let quotes = generator.generate_quotes();

            for quote in quotes {
                let _ = quote_sender.send(quote);
            }

            thread::sleep(std::time::Duration::from_millis(500));
        }
        log::info!("Поток генерации котировок остановлен");
    })
}

fn start_inactive_client_monitor(
    client_manager: Arc<Mutex<client_manager::ClientManager>>,
    running: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        log::info!("Запуск потока удаления неактивных клиентов");
        while running.load(Ordering::SeqCst) {
            thread::sleep(std::time::Duration::from_secs(5));

            let inactive_clients = client_manager
                .lock()
                .unwrap()
                .get_inactive_clients(std::time::Duration::from_secs(5));

            if !inactive_clients.is_empty() {
                for client in inactive_clients {
                    log::info!("Удаление неактивного клиента: {}", client);
                    client_manager.lock().unwrap().remove_client(client);
                }
            }
        }
        log::info!("Поток генерации неактивных клиентов остановлен");
    })
}

fn start_tcp_server(
    client_manager: Arc<Mutex<client_manager::ClientManager>>,
    quote_receiver: channel::Receiver<quote_lib::StockQuote>,
    port: u16,
    running: Arc<AtomicBool>,
) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {
    let listner = std::net::TcpListener::bind(format!("127.0.0.1:{}", port))?;
    listner.set_nonblocking(true)?;
    log::info!("TSP сервер запущен на порту {}", port);
    let mut handles = vec![];
    while running.load(Ordering::SeqCst) {
        match listner.accept() {
            Ok((stream, _)) => {
                let client_manager = client_manager.clone();
                let quote_receiver = quote_receiver.clone();
                let running_clone = running.clone();
                let handle = thread::spawn(move || {
                    command_handler::handle_client(
                        stream,
                        client_manager,
                        quote_receiver,
                        running_clone,
                    )
                });
                handles.push(handle);
            }
            Err(e) => {
                log::debug!("Error accepting connection: {}", e);
            }
        }
    }
    log::info!("TSP сервер остановлен на порту {}", port);
    Ok(handles)
}
