mod adapter;
mod host;
mod mac_tools;
mod materials;
#[cfg(target_os = "macos")]
mod macos;
mod runner;
mod store;
fn main() {
    #[cfg(target_os = "macos")]
    if let Some(result) = macos::internal_command() {
        if result.is_err() {
            std::process::exit(1);
        }
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main());
}
async fn async_main() {
    if let Err(error) = host::main().await {
        eprintln!("{}", host::safe_code(&error));
        std::process::exit(1)
    }
}
