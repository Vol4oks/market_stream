#![deny(unreachable_pub)] 
#![warn(missing_docs)]// хороший стиль!

//! Клиент для работы с сервером стриминга
//! Пример запуска:
//! cargo run -- --server-addr 127.0.0.1:8080 --udp-port 34254 --tickers-path tickers.txt

use std::{
    io::{BufRead, BufReader, Write},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use quote_lib::{PING_MSG, PONG_MSG, SERVER_OK, STREAM_CMD, StockQuote};

#[derive(Parser)]
struct Args {
    #[clap(short, long, default_value = "127.0.0.1:8080"/* сщмнительное использование портов, но для примера - сойдёт*/)]
    server_addr: String,

    #[clap(short, long, default_value = "34254")]
    udp_port: u16,

    #[clap(short, long)]
    tickers_path: String,
}

#[derive(Debug, Clone)]
struct PingData {
    addr: SocketAddr,
    last_ping: Option<Instant>,
    last_pong: Option<Instant>,
    stop: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new() // 👍
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .format_target(true)
        .init();

    // Обработка сигнала завершения
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || { // 👍
        log::info!("Получен сигнал завершения, освобождаем ресурсы...");
        r.store(false, Ordering::SeqCst);
    })?;
    let args = Args::parse();

    log::info!("Запуск клиента");

    // Читаем тикеры из файла
    let tickers = read_tickers_file(&args.tickers_path)?;
    log::info!("Загружено тикетов: {}", tickers.len());

    // Подключаемся к TCP серверу
    let mut stream = std::net::TcpStream::connect(&args.server_addr)?;
    log::info!("Подключено к серверу: {}", args.server_addr);

    // Формируем и отправляем команду STREAM
    let local_udp_addr = format!("udp://127.0.0.1:{}", args.udp_port);
    let tickers_str = tickers.join(",");
    let command = format!("{} {} {}\n", STREAM_CMD, local_udp_addr, tickers_str);

    stream.write_all(command.as_bytes())?;
    stream.flush()?;
    log::info!("Команда отправлена: {}", command.trim());
    let mut reader = BufReader::new(stream.try_clone()?);

    log::info!("Ожидаем ответа от сервера");
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => {
            log::info!("Сервер отключился");
            return Ok(());
        }
        Ok(_) => {
            if line.trim().contains(SERVER_OK) {
                log::info!("Команда выполнена успешно");
            } else {
                log::error!("Ошибка выполнения команды: {}", line.trim());
                return Err("Ошибка выполнения команды".into());
            }
        }
        Err(e) => {
            log::error!("Ошибка чтения ответа: {}", e);
            return Err(Box::new(e));
        }
    }

    // Создаем UDP сокет для приема данных
    let udp_socket = std::net::UdpSocket::bind(format!("0.0.0.0:{}", args.udp_port))?;
    log::info!("UDP сокет создан на порту: {}", args.udp_port);

    udp_socket.set_read_timeout(Some(Duration::from_secs(5)))?;

    // Определяем адрес сервера для отправки Ping
    let server_udp_addr = Arc::new(Mutex::new(None));

    // Запускаем поток для отправки Ping
    let ping_socket = udp_socket.try_clone()?;
    let server_addr_for_ping = server_udp_addr.clone();
    let running_clone = running.clone();
    let handler = thread::spawn(move || {
        send_ping_loop(ping_socket, server_addr_for_ping, running_clone);
    });

    // Основной цикл приема котировок
    receive_quotes_loop(udp_socket, server_udp_addr, running)?;
    handler.join().unwrap();
    Ok(())
}

fn read_tickers_file(path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn send_ping_loop(
    socket: std::net::UdpSocket,
    server_addr: Arc<Mutex<Option<PingData>>>,
    running: Arc<AtomicBool>,
) {
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_secs(2)); // 👍👍👍 (немногие ученики этот момент правильно делают)
        let mut server_addr = server_addr.lock().unwrap();

        if let Some(addr) = server_addr.as_mut() {
            if addr.stop {
                log::info!("Остановка отправки Ping ");
                break;
            }

            if let Err(e) = socket.send_to(PING_MSG, addr.addr) {
                log::error!("Ошибка отправки Ping: {}", e);
                break;
            }

            addr.last_ping = Some(Instant::now());
        }
    }
    log::info!("Завершение потока Ping");
}

fn receive_quotes_loop(
    socket: std::net::UdpSocket,
    server_addr: Arc<Mutex<Option<PingData>>>,
    running: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0; 1024];

    // Устанавливаем таймаут для возможности graceful shutdown
    socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    log::info!("Запускаем основной поток");
    while running.load(Ordering::SeqCst) {
        // Проверяем, не превышено ли ожидания PONG
        {
            let mut guard = server_addr.lock().unwrap();
            if let Some(addr) = guard.as_mut() {
                log::debug!("Данные по мониторингу: {:#?}", addr);
                if let Some(last_ping) = addr.last_ping {
                    if addr.last_pong.is_none() && last_ping.elapsed() > Duration::from_secs(5) {
                        addr.stop = true;
                        log::info!("Превышено время ожидания pong от сервера (5сек)");
                        break;
                    }
                    if addr.last_pong.is_some()
                        && addr.last_pong.unwrap().elapsed() > Duration::from_secs(5)
                    {
                        addr.stop = true;
                        log::info!("Превышено время ожидания pong от сервера (5сек)");
                        break;
                    }
                }
            }
        }
        match socket.recv_from(&mut buf) {
            Ok((size, src_addr)) => {
                log::info!("Получили: {} байт", size);
                // Обновляем адрес сервера, если он еще не определен
                {
                    let mut guard = server_addr.lock().unwrap();
                    if guard.is_none() {
                        *guard = Some(PingData {
                            addr: src_addr,
                            last_ping: None,
                            last_pong: None,
                            stop: false,
                        });
                        log::info!("Определен UDP-адрес сервера: {}", src_addr);
                    }
                }

                // Проверяем, не PONG ли это
                if &buf[..size] == PONG_MSG {
                    let mut guard = server_addr.lock().unwrap();
                    if let Some(ref mut addr) = *guard
                        && addr.last_ping.is_some()
                    {
                        addr.last_pong = Some(Instant::now());
                        log::info!("Получен PONG от сервера");
                    }
                    continue;
                }

                // Парсим котировку
                match StockQuote::from_string(&String::from_utf8_lossy(&buf[..size])) {
                    Ok(quote) => {
                        println!(
                            "Получена котировка: {} - ${:.2} (объем: {}) время: {}",
                            // TODO перевести время в более читабельный вид
                            quote.ticker,
                            quote.price,
                            quote.volume,
                            quote.timestamp
                        );
                    }
                    Err(e) => {
                        log::error!("Ошибка парсинга котировки: {}", e);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                log::error!("TimedOut: {}", e);
                continue;
            }

            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                log::error!("WouldBlock: {}", e);
                //
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                log::error!("Interrupted: {}", e);
                continue;
            }
            Err(e) => {
                log::error!("{}", e);
                break;
            }
        }
    }

    log::info!("Завершаем основной поток");
    Ok(())
}
