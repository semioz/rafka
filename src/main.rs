use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

// kafka response message has 3 parts: message_size, header, body
// message_size: 32-bit signed integer. It specifies the size of the header and body

// response header v0 contains single field: correlation_id which lets the client match responses with requests
// it is a 32-bit signed integer
fn handle_client(mut stream: TcpStream) {
    // buffer to read the incoming request
    let mut buffer = [0; 1024];

    // read the incoming request
    let _ = stream.read(&mut buffer).unwrap();

    // construct the response
    let message_size: i32 = 0;
    let correlation_id: i32 = 7;

    // need big endian to match the kafka protocol
    let message_size_bytes = message_size.to_be_bytes();
    let correlation_id_bytes = correlation_id.to_be_bytes();

    // construct the response
    let mut res = Vec::new();
    res.extend_from_slice(&message_size_bytes);
    res.extend_from_slice(&correlation_id_bytes);

    stream.write_all(&res).unwrap();
    stream.flush().unwrap();
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
