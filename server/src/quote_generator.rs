use std::collections::HashMap;

use quote_lib::StockQuote;
use rand::Rng;

pub(crate) struct QuoteGenerator {
    prices: HashMap<String, f64>,
    tickers: Vec<String>,
}

impl QuoteGenerator {
    pub(crate) fn new(tickers: Vec<String>) -> Self {
        let mut prices = HashMap::new();
        let mut rng = rand::rng();
        for ticker in &tickers {
            prices.insert(ticker.clone(), rng.random_range(100.0..1000.0));
        }

        QuoteGenerator { prices, tickers }
    }

    pub(crate) fn generate_quotes(&mut self) -> Vec<StockQuote> {
        let mut quotes = Vec::new();

        for ticker in &self.tickers.clone() {
            if let Some(quote) = self.generate_quote(ticker) {
                quotes.push(quote);
            }
        }
        quotes
    }

    fn generate_quote(&mut self, ticker: &str) -> Option<StockQuote> {
        let price = self.prices.get_mut(ticker)?;
        *price += rand::rng().random_range(-5.0..5.0);

        if *price < 1.0 {
            *price = 1.0;
        }

        let volume: f64 = match ticker {
            "AAPL" | "MSFT" | "GOOGL" | "TSLA" => 1000_f64 + (rand::random::<f64>() * 5000.0),
            _ => 100_f64 + (rand::random::<f64>() * 1000.0),
        };

        Some(StockQuote {
            ticker: ticker.to_string(),
            price: *price,
            volume,
            timestamp: quote_lib::get_timestamp(),
        })
    }
}
