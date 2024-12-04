use differential_dataflow::input::InputSession;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use rand::distributions::{Distribution, Uniform};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;

// Represents a single trade
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
struct Trade {
    timestamp: i64,
    market: u32,
    price: Decimal,
}

// Statistics of trades for a given hour
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
struct FHLLVData {
    timestamp: i64,
    market: u32,
    high: Decimal,
}

fn round_down_to_hour(ts: i64) -> i64 {
    (ts / 3600) * 3600
}

fn generate_synthetic_trades(input: &mut InputSession<i64, Trade, isize>) -> () {
    const NUM_MARKETS: usize = 700;
    const NUM_TRADES: usize = 20_000_000;
    const START_TIME: i64 = 1717192800; // 2024-06-01 00:00:00 UTC
    const END_TIME: i64 = 1735599599; // 2024-12-31 23:59:59 UTC

    let mut rng = rand::thread_rng();

    // Trades are unevenly distributed across markets
    let mut market_weights: Vec<f64> = (1..=NUM_MARKETS)
        .map(|i| 1.0 / (i as f64).powf(1.3))
        .collect();

    // Normalize weights so they sum to 1
    let sum: f64 = market_weights.iter().sum();
    market_weights.iter_mut().for_each(|w| *w /= sum);

    // Calculate exact number of trades per market
    let trades_per_market: Vec<usize> = market_weights
        .iter()
        .map(|&w| (w * NUM_TRADES as f64).round() as usize)
        .collect();

    // Create uniform distribution for timestamps
    let time_dist = Uniform::new_inclusive(START_TIME, END_TIME);

    // Create price distribution
    let price_dist = Uniform::new(1.0, 100000.0);

    // Generate trades for each market
    for (market_idx, &num_trades) in trades_per_market.iter().enumerate() {
        let market = market_idx as u32;

        for _ in 0..num_trades {
            // Generate random timestamp within range
            let timestamp = time_dist.sample(&mut rng);

            // Generate price and volume
            let price = Decimal::from_f64_retain(price_dist.sample(&mut rng)).unwrap();

            let trade = Trade {
                timestamp,
                market,
                price,
            };

            input.insert(trade);
        }
    }

    input.advance_to(1);
    input.flush();
}

fn main() -> () {
    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();
        let mut input = InputSession::new();

        worker.dataflow(|scope| {
            let trades = input.to_collection(scope);
            let hourly_data = compute_hourly_data(&trades);
            hourly_data.inspect(move |x| println!("HOURLY: {:?}", x));
        });

        if index == 0 {
            generate_synthetic_trades(&mut input);
        }

        Ok::<(), ()>(())
    })
    .expect("Computation failed");
}

fn compute_hourly_data<G: Scope<Timestamp = i64>>(
    trades: &Collection<G, Trade, isize>,
) -> Collection<G, FHLLVData, isize> {
    // Group trades by market and hour
    trades
        .map(|trade| {
            let hour_ts = round_down_to_hour(trade.timestamp);
            ((hour_ts, trade.market.clone()), trade.price)
        })
        .reduce(|(hour_ts, market), input, output| {
            let mut high_price = Decimal::MIN;

            for (price, _count) in input.iter() {
                high_price = high_price.max(**price);
            }

            let ohlcv = FHLLVData {
                timestamp: *hour_ts,
                market: *market,
                high: high_price,
            };
            output.push((ohlcv, 1));
        })
        .map(|(_, ohlcv)| ohlcv)
}
