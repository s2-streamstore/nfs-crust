use std::env;

use bytes::Bytes;
use nfs_crust::{Error, NfsClient, PutMode};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let endpoint = env::var("NFS_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:2049".to_owned());
    let export = env::var("NFS_EXPORT").unwrap_or_else(|_| "/".to_owned());
    let client = NfsClient::builder(endpoint, export).connect().await?;

    client
        .put(
            "demo/hello.txt",
            Bytes::from_static(b"hello from nfs-crust"),
            PutMode::IfNotExists,
        )
        .await?;

    let body = client.get("demo/hello.txt").await?;
    println!("{}", String::from_utf8_lossy(&body));

    let mut continuation_token = None;
    loop {
        let page = client
            .list_page("demo/", 100, continuation_token.take())
            .await?;
        for entry in page.entries {
            let info = client.entry_info(&entry.path).await?;
            println!("{} {} bytes", entry.path, info.size);
        }
        continuation_token = page.next_token;
        if continuation_token.is_none() {
            break;
        }
    }

    client.delete("demo/hello.txt").await?;
    Ok(())
}
