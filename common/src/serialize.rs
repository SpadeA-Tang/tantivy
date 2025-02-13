use async_trait::async_trait;
use std::fmt;
use std::{borrow::Cow, pin, task::Poll};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::VInt;

#[derive(Default)]
struct Counter(u64);

impl AsyncWrite for Counter {
    fn poll_write(
        self: pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.get_mut().0 += buf.len() as u64;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Trait for a simple binary serialization.
#[async_trait]
pub trait BinarySerializable: fmt::Debug + Sized {
    /// Serialize
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()>;
    /// Deserialize
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self>;

    async fn num_bytes(&self) -> u64 {
        let mut counter = Counter::default();
        self.serialize(&mut counter).await.unwrap();
        counter.0
    }
}

#[async_trait]
pub trait DeserializeFrom<T: BinarySerializable> {
    async fn deserialize(&mut self) -> io::Result<T>;
}

/// Implement deserialize from &[u8] for all types which implement BinarySerializable.
///
/// TryFrom would actually be preferable, but not possible because of the orphan
/// rules (not completely sure if this could be resolved)
#[async_trait]
impl<T: BinarySerializable> DeserializeFrom<T> for &[u8] {
    async fn deserialize(&mut self) -> io::Result<T> {
        T::deserialize(self).await
    }
}

/// `FixedSize` marks a `BinarySerializable` as
/// always serializing to the same size.
pub trait FixedSize: BinarySerializable {
    const SIZE_IN_BYTES: usize;
}

#[async_trait]
impl BinarySerializable for () {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(&self, _: &mut W) -> io::Result<()> {
        Ok(())
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(_: &mut R) -> io::Result<Self> {
        Ok(())
    }
}

impl FixedSize for () {
    const SIZE_IN_BYTES: usize = 0;
}

#[async_trait]
impl<T: BinarySerializable + Send + Sync> BinarySerializable for Vec<T> {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        BinarySerializable::serialize(&VInt(self.len() as u64), writer).await?;
        for it in self {
            it.serialize(writer).await?;
        }
        Ok(())
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Vec<T>> {
        let num_items = <VInt as BinarySerializable>::deserialize(reader)
            .await?
            .val();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = T::deserialize(reader).await?;
            items.push(item);
        }
        Ok(items)
    }
}

#[async_trait]
impl<Left: BinarySerializable + Send + Sync, Right: BinarySerializable + Send + Sync>
    BinarySerializable for (Left, Right)
{
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        write: &mut W,
    ) -> io::Result<()> {
        self.0.serialize(write).await?;
        self.1.serialize(write).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        Ok((
            Left::deserialize(reader).await?,
            Right::deserialize(reader).await?,
        ))
    }
}

impl<
        Left: BinarySerializable + FixedSize + Send + Sync,
        Right: BinarySerializable + FixedSize + Send + Sync,
    > FixedSize for (Left, Right)
{
    const SIZE_IN_BYTES: usize = Left::SIZE_IN_BYTES + Right::SIZE_IN_BYTES;
}

#[async_trait]
impl BinarySerializable for u32 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u32_le(*self).await
    }

    async fn deserialize<R: AsyncRead + Send + Unpin>(reader: &mut R) -> io::Result<u32> {
        reader.read_u32_le().await
    }
}

impl FixedSize for u32 {
    const SIZE_IN_BYTES: usize = 4;
}

#[async_trait]
impl BinarySerializable for u16 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u16_le(*self).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<u16> {
        reader.read_u16_le().await
    }
}

impl FixedSize for u16 {
    const SIZE_IN_BYTES: usize = 2;
}

#[async_trait]
impl BinarySerializable for u64 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u64_le(*self).await
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        reader.read_u64_le().await
    }
}

impl FixedSize for u64 {
    const SIZE_IN_BYTES: usize = 8;
}

#[async_trait]
impl BinarySerializable for u128 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u128_le(*self).await
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        reader.read_u128_le().await
    }
}

impl FixedSize for u128 {
    const SIZE_IN_BYTES: usize = 16;
}

#[async_trait]
impl BinarySerializable for f32 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_f32_le(*self).await
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        reader.read_f32_le().await
    }
}

impl FixedSize for f32 {
    const SIZE_IN_BYTES: usize = 4;
}

#[async_trait]
impl BinarySerializable for i64 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_i64_le(*self).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        reader.read_i64_le().await
    }
}

impl FixedSize for i64 {
    const SIZE_IN_BYTES: usize = 8;
}

#[async_trait]
impl BinarySerializable for f64 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_f64_le(*self).await
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Self> {
        reader.read_f64_le().await
    }
}

impl FixedSize for f64 {
    const SIZE_IN_BYTES: usize = 8;
}
#[async_trait]
impl BinarySerializable for u8 {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u8(*self).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<u8> {
        reader.read_u8().await
    }
}

impl FixedSize for u8 {
    const SIZE_IN_BYTES: usize = 1;
}
#[async_trait]
impl BinarySerializable for bool {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        writer.write_u8(u8::from(*self)).await
    }
    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<bool> {
        let val = reader.read_u8().await?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid bool value on deserialization, data corrupted",
            )),
        }
    }
}

impl FixedSize for bool {
    const SIZE_IN_BYTES: usize = 1;
}
#[async_trait]
impl BinarySerializable for String {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        let data: &[u8] = self.as_bytes();
        BinarySerializable::serialize(&VInt(data.len() as u64), writer).await?;
        writer.write_all(data).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<String> {
        let string_length = <VInt as BinarySerializable>::deserialize(reader)
            .await?
            .val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)
            .await?;
        Ok(result)
    }
}

#[async_trait]
impl<'a> BinarySerializable for Cow<'a, str> {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        let data: &[u8] = self.as_bytes();
        BinarySerializable::serialize(&VInt(data.len() as u64), writer).await?;
        writer.write_all(data).await
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Cow<'a, str>> {
        let string_length = <VInt as BinarySerializable>::deserialize(reader)
            .await?
            .val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)
            .await?;
        Ok(Cow::Owned(result))
    }
}

#[async_trait]
impl<'a> BinarySerializable for Cow<'a, [u8]> {
    async fn serialize<W: AsyncWrite + ?Sized + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> io::Result<()> {
        BinarySerializable::serialize(&VInt(self.len() as u64), writer).await?;
        for it in self.iter() {
            BinarySerializable::serialize(it, writer).await?;
        }
        Ok(())
    }

    async fn deserialize<R: AsyncRead + Unpin + Send>(reader: &mut R) -> io::Result<Cow<'a, [u8]>> {
        let num_items = <VInt as BinarySerializable>::deserialize(reader)
            .await?
            .val();
        let mut items: Vec<u8> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = <u8 as BinarySerializable>::deserialize(reader).await?;
            items.push(item);
        }
        Ok(Cow::Owned(items))
    }
}

#[cfg(test)]
pub mod test {

    use super::*;
    pub fn fixed_size_test<O: BinarySerializable + FixedSize + Default>() {
        let mut buffer = Vec::new();
        O::default().serialize(&mut buffer).unwrap();
        assert_eq!(buffer.len(), O::SIZE_IN_BYTES);
    }

    fn serialize_test<T: BinarySerializable + Eq>(v: T) -> usize {
        let mut buffer: Vec<u8> = Vec::new();
        v.serialize(&mut buffer).unwrap();
        let num_bytes = buffer.len();
        let mut cursor = &buffer[..];
        let deser = T::deserialize(&mut cursor).unwrap();
        assert_eq!(deser, v);
        num_bytes
    }

    #[test]
    fn test_serialize_u8() {
        fixed_size_test::<u8>();
    }

    #[test]
    fn test_serialize_u32() {
        fixed_size_test::<u32>();
        assert_eq!(4, serialize_test(3u32));
        assert_eq!(4, serialize_test(5u32));
        assert_eq!(4, serialize_test(u32::MAX));
    }

    #[test]
    fn test_serialize_i64() {
        fixed_size_test::<i64>();
    }

    #[test]
    fn test_serialize_f64() {
        fixed_size_test::<f64>();
    }

    #[test]
    fn test_serialize_u64() {
        fixed_size_test::<u64>();
    }

    #[test]
    fn test_serialize_bool() {
        fixed_size_test::<bool>();
    }

    #[test]
    fn test_serialize_string() {
        assert_eq!(serialize_test(String::from("")), 1);
        assert_eq!(serialize_test(String::from("ぽよぽよ")), 1 + 3 * 4);
        assert_eq!(serialize_test(String::from("富士さん見える。")), 1 + 3 * 8);
    }

    #[test]
    fn test_serialize_vec() {
        assert_eq!(serialize_test(Vec::<u8>::new()), 1);
        assert_eq!(serialize_test(vec![1u32, 3u32]), 1 + 4 * 2);
    }

    #[test]
    fn test_serialize_vint() {
        for i in 0..10_000 {
            serialize_test(VInt(i as u64));
        }
        assert_eq!(serialize_test(VInt(7u64)), 1);
        assert_eq!(serialize_test(VInt(127u64)), 1);
        assert_eq!(serialize_test(VInt(128u64)), 2);
        assert_eq!(serialize_test(VInt(129u64)), 2);
        assert_eq!(serialize_test(VInt(1234u64)), 2);
        assert_eq!(serialize_test(VInt(16_383u64)), 2);
        assert_eq!(serialize_test(VInt(16_384u64)), 3);
        assert_eq!(serialize_test(VInt(u64::MAX)), 10);
    }
}
