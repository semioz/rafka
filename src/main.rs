use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

// kafka response message has 3 parts: message_size, header, body
// message_size: 32-bit signed integer. It specifies the size of the header and body

// response header v0 contains single field: correlation_id which lets the client match responses with requests
// it is a 32-bit signed integer
fn extract_corr_id(buff: &[u8]) -> i32 {
    if buff.len() < 8 {
        eprintln!("Request too short to contain a correlation_id.");
        return 0;
    }

    let mut corr_id_bytes = [0; 4];
    corr_id_bytes.copy_from_slice(&buff[4..8]);
    i32::from_be_bytes(corr_id_bytes)
}

// response header v0 contains single field: correlation_id which lets the client match responses with requests
// it is a 32-bit signed integer
fn handle_client(mut stream: TcpStream) {
    // reading exact 4 bytes for message size
    let mut size_buffer = [0; 4];
    if stream.read_exact(&mut size_buffer).is_err() {
        eprintln!("Failed to read message size");
        return;
    }

    let message_size = i32::from_be_bytes(size_buffer);

    if message_size <= 0 || message_size as usize > 1024 {
        eprintln!("Invalid message size: {}", message_size);
        return;
    }

    // read the remaining bytes based on the message size
    let mut buffer = vec![0; message_size as usize];
    if stream.read_exact(&mut buffer).is_err() {
        eprintln!("Failed to read message");
        return;
    }

    let correlation_id = extract_corr_id(&buffer);
    // need big endian to match the kafka protocol
    let correlation_id_bytes = correlation_id.to_be_bytes();

    // construct the response
    let mut res = Vec::new();
    res.extend_from_slice(&[0, 0, 0, 0]);
    res.extend_from_slice(&correlation_id_bytes);

    // write the response back to the client
    match stream.write_all(&res) {
        Ok(_) => {
            println!("response sent successfully");
        }
        Err(e) => {
            eprintln!("Failed to send response: {}", e);
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    println!("Starting server on port 9092");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
