//! Row format v1: header (txn_id, tombstone) + binary-encoded columns.
//! Types: INT (8 bytes LE), TEXT (4-byte length + UTF-8), BOOL (1 byte).

use anyhow::{ensure, Result};
use std::io::{Cursor, Read, Write};

pub const ROW_HEADER_LEN: usize = 9; // txn_id (8) + tombstone (1)

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

/// Encode a row: header (txn_id, tombstone) then column values per schema.
/// Tombstone 0 = live, 1 = deleted.
pub fn encode(
    schema: &[ColumnType],
    values: &[Value],
    txn_id: u64,
    tombstone: u8,
) -> Result<Vec<u8>> {
    ensure!(schema.len() == values.len(), "schema len != values len");
    let mut buf = Vec::with_capacity(ROW_HEADER_LEN + 64);
    buf.write_all(&txn_id.to_le_bytes())?;
    buf.write_all(&[tombstone])?;
    for (ty, v) in schema.iter().zip(values.iter()) {
        encode_value(&mut buf, ty, v)?;
    }
    Ok(buf)
}

/// Decode a row. Returns (txn_id, tombstone, values).
pub fn decode(schema: &[ColumnType], bytes: &[u8]) -> Result<(u64, u8, Vec<Value>)> {
    ensure!(bytes.len() >= ROW_HEADER_LEN, "row too short");
    let mut c = Cursor::new(bytes);
    let mut txn_buf = [0u8; 8];
    c.read_exact(&mut txn_buf)?;
    let txn_id = u64::from_le_bytes(txn_buf);
    let mut tombstone_buf = [0u8; 1];
    c.read_exact(&mut tombstone_buf)?;
    let tombstone = tombstone_buf[0];
    let mut values = Vec::with_capacity(schema.len());
    for ty in schema {
        values.push(decode_value(&mut c, ty)?);
    }
    Ok((txn_id, tombstone, values))
}

fn encode_value<W: Write>(w: &mut W, ty: &ColumnType, v: &Value) -> Result<()> {
    match (ty, v) {
        (ColumnType::Int, Value::Int(n)) => w.write_all(&n.to_le_bytes())?,
        (ColumnType::Text, Value::Text(s)) => {
            let b = s.as_bytes();
            w.write_all(&(b.len() as u32).to_le_bytes())?;
            w.write_all(b)?;
        }
        (ColumnType::Bool, Value::Bool(b)) => w.write_all(&[if *b { 1 } else { 0 }])?,
        _ => anyhow::bail!("type mismatch: {:?} vs {:?}", ty, v),
    }
    Ok(())
}

fn decode_value<R: Read>(r: &mut R, ty: &ColumnType) -> Result<Value> {
    match ty {
        ColumnType::Int => {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(Value::Int(i64::from_le_bytes(b)))
        }
        ColumnType::Text => {
            let mut len_b = [0u8; 4];
            r.read_exact(&mut len_b)?;
            let len = u32::from_le_bytes(len_b) as usize;
            let mut b = vec![0u8; len];
            r.read_exact(&mut b)?;
            let s = String::from_utf8(b).map_err(|e| anyhow::anyhow!("invalid utf8: {}", e))?;
            Ok(Value::Text(s))
        }
        ColumnType::Bool => {
            let mut b = [0u8; 1];
            r.read_exact(&mut b)?;
            Ok(Value::Bool(b[0] != 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_int_text_bool() -> Vec<ColumnType> {
        vec![ColumnType::Int, ColumnType::Text, ColumnType::Bool]
    }

    #[test]
    fn encode_decode_roundtrip() {
        let schema = schema_int_text_bool();
        let values = vec![
            Value::Int(42),
            Value::Text("hello".to_string()),
            Value::Bool(true),
        ];
        let encoded = encode(&schema, &values, 1, 0).unwrap();
        let (txn, tomb, decoded) = decode(&schema, &encoded).unwrap();
        assert_eq!(txn, 1);
        assert_eq!(tomb, 0);
        assert_eq!(decoded, values);
    }

    #[test]
    fn tombstone_deleted() {
        let schema = schema_int_text_bool();
        let values = vec![Value::Int(0), Value::Text("x".to_string()), Value::Bool(false)];
        let encoded = encode(&schema, &values, 99, 1).unwrap();
        let (txn, tomb, _) = decode(&schema, &encoded).unwrap();
        assert_eq!(txn, 99);
        assert_eq!(tomb, 1);
    }

    #[test]
    fn empty_text() {
        let schema = vec![ColumnType::Text];
        let values = vec![Value::Text(String::new())];
        let encoded = encode(&schema, &values, 0, 0).unwrap();
        let (_, _, decoded) = decode(&schema, &encoded).unwrap();
        assert_eq!(decoded, values);
    }
}
