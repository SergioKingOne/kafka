use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};
use thiserror::Error;

#[derive(Error, Debug)]
enum KafkaErrors {
    #[error("Failed to read from stream: {0}")]
    ReadError(#[from] std::io::Error),
}

/// Kafka response message
struct Response {
    /// Specifies size of header and body
    message_size: i32,
    /// Helps clients match their original requests
    correlation_id: i32,
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
    fn from(stream: &mut TcpStream) -> Result<Self, KafkaErrors> {
        // Read the message size (4 bytes)
        let mut size_bytes = [0u8; 4];
        stream.read_exact(&mut size_bytes)?;
        let message_size = i32::from_be_bytes(size_bytes);

        // Read the API key (2 bytes)
        let mut api_key_bytes = [0u8, 2];
        stream.read_exact(&mut api_key_bytes)?;
        let request_api_key = u16::from_be_bytes(api_key_bytes);

        // Read the API version (2 bytes)
        let mut api_version_bytes = [0u8; 2];
        stream.read_exact(&mut api_version_bytes)?;
        let reques_api_version = u16::from_be_bytes(api_version_bytes);

        // Read correlation ID (4 bytes)
        let mut correlation_bytes = [0u8; 4];
        stream.read_exact(&mut correlation_bytes)?;
        let correlation_id = i32::from_be_bytes(correlation_bytes);

        Ok(Request::builder()
            .message_size(message_size)
            .request_api_key(request_api_key)
            .request_api_version(reques_api_version)
            .correlation_id(correlation_id)
            .build())
    }
}

fn main() -> Result<(), KafkaErrors> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("Accepted new connection");

                let request = match Request::from(&mut stream) {
                    Ok(request) => request,
                    Err(err) => {
                        println!("Could not parse request: {}", err);
                        continue;
                    }
                };

                println!(
                    "Received request with correaltion ID: {}",
                    request.correlation_id
                );

                let response = Response {
                    message_size: 0,
                    correlation_id: request.correlation_id,
                };

                let size_bytes = response.message_size.to_be_bytes();
                let correlation_bytes = response.correlation_id.to_be_bytes();

                // Write the response to the stream
                stream.write_all(&size_bytes).unwrap();
                stream.write_all(&correlation_bytes).unwrap();
                stream.flush().unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
