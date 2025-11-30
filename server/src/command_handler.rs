use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpStream, UdpSocket},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::Receiver;
use quote_lib::{PING_MSG, PONG_MSG, SERVER_OK, STREAM_CMD, StockQuote};

use crate::client_manager::ClientManager;

pub(crate) fn handle_client(
    stream: TcpStream,
    client_manager: Arc<Mutex<ClientManager>>,
    broadcast_receiver: Receiver<StockQuote>,
    running: Arc<AtomicBool>,
) {
    let peer_addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(e) => {
            log::error!("Failed to get peer address: {}", e);
            return;
        }
    };
    log::info!("Новый клиент: {}", peer_addr);

    let stream_clone = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to clone stream: {}", e);
            return;
        }
    };

    let mut wr_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to clone stream: {}", e);
            return;
        }
    };

    let mut reader = BufReader::new(stream_clone);
    let mut handles = vec![];

    while running.load(Ordering::SeqCst) {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                log::info!("Клиент отключился: {}", peer_addr);
                break;
            }
            Ok(_) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                log::info!("Запрос {}: {}", peer_addr, line);
                let mut response = SERVER_OK.to_string();
                if let Some((client_id, handle)) = process_command(
                    line,
                    &peer_addr,
                    &client_manager,
                    broadcast_receiver.clone(),
                    running.clone(),
                ) {
                    log::info!("Запуск команды от клиента: {}", client_id);
                    handles.push(handle);
                } else {
                    response = format!("Неизвестная команда: {}", line);
                }
                response.push('\n');
                if let Err(e) = wr_stream.write_all(response.as_bytes()) {
                    log::error!("Error sending response: {} ", e);
                    break;
                }
                let _ = wr_stream.flush();
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    continue;
                } else {
                    log::error!("Failed to read from stream: {}", e);
                    break;
                }
            }
        }
    }
    for handle in handles {
        handle.join().unwrap();
    }
    log::info!("Завершение потока обработки команд: {}", peer_addr);
}

fn process_command(
    command: &str,
    client_addr: &std::net::SocketAddr,
    client_manager: &Arc<Mutex<ClientManager>>,
    broadcast_sender: Receiver<StockQuote>,
    running: Arc<AtomicBool>,
) -> Option<(u64, JoinHandle<()>)> {
    let parts: Vec<&str> = command.split_whitespace().collect();

    if parts.len() < 3 || parts[0] != STREAM_CMD {
        log::error!("Некорректная команда от {}: {}", client_addr, command);
        return None;
    }

    let udp_url = parts[1];
    let udp_addr = parse_udp_address(udp_url)?;

    let tickers: Vec<String> = parts[2].split(',').map(|s| s.trim().to_string()).collect();
    if tickers.is_empty() {
        return None;
    }

    let client_id = {
        let mut manager = client_manager.lock().unwrap();
        manager.add_client(tickers)
    };

    let handle = start_client_stream_thread(
        client_id,
        udp_addr,
        broadcast_sender,
        client_manager.clone(),
        running,
    );

    Some((client_id, handle))
}

fn parse_udp_address(udp_url: &str) -> Option<std::net::SocketAddr> {
    if let Some(addr_str) = udp_url.strip_prefix("udp://") {
        addr_str.parse().ok()
    } else {
        udp_url.parse().ok()
    }
}

fn start_client_stream_thread(
    client_id: u64,
    udp_addr: std::net::SocketAddr,
    receiver: Receiver<StockQuote>,
    client_manager: Arc<Mutex<ClientManager>>,
    running: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        log::info!("Запуск потока для {} на {}", client_id, udp_addr);

        let udp_socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to bind UDP socket: {}", e);
                return;
            }
        };

        if let Err(e) = udp_socket.set_write_timeout(Some(std::time::Duration::from_secs(5))) {
            log::error!("Error setting send timeout for client {}: {}", client_id, e);
        }

        let ping_socket = match udp_socket.try_clone() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to clone UDP socket for ping: {}", e);
                return;
            }
        };

        let ping_manager = client_manager.clone();
        let handle = thread::spawn(move || {
            handle_ping_messages(ping_socket, client_id, ping_manager, running);
        });

        while let Ok(quote) = receiver.recv() {
            if client_manager
                .lock()
                .unwrap()
                .get_inactive_clients(Duration::from_secs(3))
                .contains(&client_id)
            {
                log::info!("Остановка потока для {} на {}", client_id, udp_addr);
                break;
            }
            log::debug!("Обрабатываем: {}", quote.ticker);

            // проверяем, что клиент подписан на тикер
            if !client_manager
                .lock()
                .unwrap()
                .check_client_ticker(client_id, &quote.ticker)
                .unwrap_or(false)
            {
                continue;
            }

            let data = quote.to_string();
            log::debug!("Отправляем {} на адрес {}", data, udp_addr);
            if let Err(e) = udp_socket.send_to(data.as_bytes(), udp_addr) {
                log::error!("Failed to send UDP data to {}: {}", udp_addr, e);
                break;
            }

            thread::sleep(Duration::from_millis(100));
        }

        handle.join().unwrap();
        log::info!("Поток остановлен для {} на {}", client_id, udp_addr);
    })
}

fn handle_ping_messages(
    socket: UdpSocket,
    client_id: u64,
    client_manager: Arc<Mutex<ClientManager>>,
    running: Arc<AtomicBool>,
) {
    let mut buffer = [0; 1024];
    log::info!("Запуск потока для Ping для клиента {}", client_id);

    if let Err(e) = socket.set_read_timeout(Some(Duration::from_secs(5))) {
        log::error!(
            "Failed to set read timeout for ping socket for client {}: {}",
            client_id,
            e
        );
        return;
    }

    while running.load(Ordering::SeqCst) {
        if !client_manager
            .lock()
            .unwrap()
            .clients
            .read()
            .unwrap()
            .contains_key(&client_id)
        {
            log::info!("Завершение потока для Ping для клиента {}", client_id);
            break;
        }
        match socket.recv_from(&mut buffer) {
            Ok((size, src_addr)) => {
                if &buffer[..size] == PING_MSG {
                    if client_manager.lock().unwrap().update_ping(client_id) {
                        if let Err(e) = socket.send_to(PONG_MSG, src_addr) {
                            log::error!(
                                "Failed to send PONG message to client {}: {}",
                                client_id,
                                e
                            );
                        };
                    } else {
                        log::error!(
                            "Failed to update ping time for client {}: client not found",
                            client_id
                        );
                        break;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                if !client_manager
                    .lock()
                    .unwrap()
                    .clients
                    .read()
                    .unwrap()
                    .contains_key(&client_id)
                {
                    break;
                }
                continue;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                continue;
            }
            Err(e) => {
                log::error!(
                    "Failed to receive ping message for client {}: {}",
                    client_id,
                    e
                );
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    log::info!("Поток остановлен для Ping для клиента {}", client_id);
}
