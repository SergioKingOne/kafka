use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
enum KafkaError {
    #[error("Failed to read from stream: {0}")]
    ReadError(#[from] std::io::Error),
}

// TODO: Parse API Version.
// 1. Diff APIs based on api key key.
// 2. Diff request bodies for every API.
// 3. Response code for each event. If writes succeeded - for Produce API.
// 4. request_api_version in header specify the API version used.
// 5. API Versions: indicates what versions the broker supports.

/// Kafka response message
struct Response {
    /// Specifies size of header and body
    message_size: i32,
    /// Helps clients match their original requests
    correlation_id: i32,
}

enum RequestApi {
    ApiVersions,
}

/// Kafka request message
#[derive(bon::Builder)]
struct Request {
    /// Specifies size of header and body
    message_size: i32,
    /// The API key for the request
    request_api_key: u16,
    /// The version of the API for the request
    request_api_version: u16,
    /// A unique identifier for the request
    correlation_id: i32,
    /// The client ID for the request
    client_id: Option<String>,
    /// Optional tagged fields
    #[builder(default)]
    tag_buffer: Vec<String>,
}

impl Request {
    fn from(stream: &mut TcpStream) -> Result<Self, KafkaError> {
        // Read the message size (4 bytes)
        let mut size_bytes = [0u8; 4];
        stream.read_exact(&mut size_bytes)?;
        let message_size = i32::from_be_bytes(size_bytes);
        debug!("Message size: {}", message_size);

        // Read the API key (2 bytes)
        let mut api_key_bytes = [0u8, 2];
        stream.read_exact(&mut api_key_bytes)?;
        let request_api_key = u16::from_be_bytes(api_key_bytes);
        debug!("Request API key: {}", request_api_key);

        // Read the API version (2 bytes)
        let mut api_version_bytes = [0u8; 2];
        stream.read_exact(&mut api_version_bytes)?;
        let request_api_version = u16::from_be_bytes(api_version_bytes);
        debug!("Request API version: {}", request_api_version);

        // Read correlation ID (4 bytes)
        let mut correlation_bytes = [0u8; 4];
        stream.read_exact(&mut correlation_bytes)?;
        let correlation_id = i32::from_be_bytes(correlation_bytes);
        debug!("Correlation ID: {}", correlation_id);

        Ok(Request::builder()
            .message_size(message_size)
            .request_api_key(request_api_key)
            .request_api_version(request_api_version)
            .correlation_id(correlation_id)
            .build())
    }
}

fn main() -> Result<(), KafkaError> {
    // Initialize tracing subscriber with RUST_LOG env var, defaulting to "info"
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    info!("Starting Kafka server...");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    info!("Listening on 127.0.0.1:9092");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                info!("Accepted new connection");

                let request = match Request::from(&mut stream) {
                    Ok(request) => request,
                    Err(err) => {
                        error!("Could not parse request: {}", err);
                        continue;
                    }
                };

                info!(correlation_id = request.correlation_id, "Received request");

                let response = Response {
                    message_size: 0,
                    correlation_id: request.correlation_id,
                };

                let size_bytes = response.message_size.to_be_bytes();
                let correlation_bytes = response.correlation_id.to_be_bytes();

                // Write the response to the stream
                stream.write_all(&size_bytes).unwrap();
                stream.write_all(&correlation_bytes).unwrap();

                if let Err(e) = stream.flush() {
                    error!("Failed to flush stream: {}", e);
                }
            }
            Err(e) => {
                error!("Connection error: {}", e);
            }
        }
    }

    Ok(())
}
