//! Print the component spec material derived from a native Supabase repository.

use std::env;
use std::io::Write as _;
use std::path::PathBuf;

use henosis_supabase_authoring::derive_component;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repository = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let component = derive_component(repository)?;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &component)?;
    stdout.write_all(b"\n")?;
    Ok(())
}
