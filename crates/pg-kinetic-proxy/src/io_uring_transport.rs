use bytes::{BufMut, BytesMut};
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent},
    net::TcpStream,
};

#[derive(Debug)]
pub struct MonoioTransport {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl MonoioTransport {
    #[must_use]
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: vec![0; 16 * 1024],
        }
    }

    pub async fn read_into(&mut self, dst: &mut BytesMut) -> std::io::Result<usize> {
        let buffer = std::mem::take(&mut self.read_buf);
        let (result, buffer) = self.stream.read(buffer).await;
        self.read_buf = buffer;
        let read = result?;
        if read > 0 {
            dst.put_slice(&self.read_buf[..read]);
        }
        Ok(read)
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            let chunk = bytes[written..].to_vec();
            let (result, chunk) = self.stream.write(chunk).await;
            let n = result?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "monoio write returned zero",
                ));
            }
            written += n;
            drop(chunk);
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.stream.shutdown().await
    }
}
