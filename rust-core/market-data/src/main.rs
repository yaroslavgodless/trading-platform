use tokio;

mod market_data;

#[tokio::main]
async fn main() {
    println!("Trading Core starting...");
    
    let market_handle = tokio::spawn(market_data::start_market_data_service()); _ = tokio::join!(market_handle);
}
