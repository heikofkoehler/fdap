#[path = "../../examples/05_fdap_query_engine.rs"]
mod showcase;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    showcase::run_full_fdap_showcase().await
}
