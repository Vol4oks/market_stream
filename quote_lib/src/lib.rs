#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Клиент-серверная библиотека для обмена сообщениями о котировках акций.

/// Котировка акции.
#[derive(Debug, Clone)]
pub struct StockQuote {
    /// Тикер акции.
    pub ticker: String,
    /// Цена акции.
    pub price: f64, // в реальном финтехе никогда цена не моделируется через числа с плавающими точками из-за небольших ошибок при операциях над ними) знай теперь)
    /// Объем торгов.
    pub volume: f64,
    /// Время торгов.
    pub timestamp: u64, // оучше испольщовать std::time::Date / chrono::DateTime типизировынные обёртки над сырыми юникс датами
}

impl StockQuote {
    /// Создание котировки из строки.
    pub fn from_string(s: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let l: Vec<&str> = s.split("|").collect();
        if l.len() != 4 {
            return Err(format!("Expected 4 fields, got {}: {:?}", l.len(), l).into());
        }
        Ok(StockQuote {
            ticker: l[0].to_string(),
            price: l[1].parse()?,
            volume: l[2].parse()?,
            timestamp: l[3].parse()?,
        })
    }
}

impl std::fmt::Display for StockQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp
        )
    }
}
/// Команда клиента для мониторинга.
pub const PING_MSG: &[u8] = b"PING";
/// Команда сервера для подтверждения.
pub const PONG_MSG: &[u8] = b"PONG";
/// Команда клиента для потока котировок.
pub const STREAM_CMD: &str = "STREAM";
/// Ответ сервера.
pub const SERVER_OK: &str = "OK";

/// Получение текущего времени в секундах.
pub fn get_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Error get_timestamp")
        .as_secs()
}
