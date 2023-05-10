//! Database connection and struct representing rows in the data tables.

use rusqlite::{params, Connection, Row};
use std::net::SocketAddr;
use std::path::Path;
use tracing::warn;

/// Wrapper around database connection
pub(crate) struct Db {
    conn: Connection,
}

impl Db {
    pub(crate) fn new(conn: Connection) -> Self {
        Self { conn }
    }

    /// Opens an existing SQLite Db or creates it
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        warn!("Lets try to open database at {:?}", path);
        let conn = Connection::open(path)?;
        let init_sql = include_str!("init.sql");
        conn.execute_batch(init_sql)?;
        warn!("Opened database at {:?}", path);
        Ok(Self::new(conn))
    }
}

pub struct TransactionRow {
    pub address: SocketAddr,
    pub peer_id: String,
    pub is_forwarded: u8,
    pub signer_id: String,
    pub receiver_id: String,
}

pub struct TestRow {
    pub pes_id: String,
}

impl TestRow {
    const SELECT_ALL: &'static str = "pes_id";
    pub(crate) fn get_any_row(db: &Db) -> anyhow::Result<Vec<Self>> {
        let select: &str = Self::SELECT_ALL;
        let mut stmt = db.conn.prepare(&format!("SELECT {select} FROM test"))?;
        let data =
            stmt.query_map([], Self::from_row)?.collect::<Result<Vec<_>, rusqlite::Error>>()?;
        Ok(data)
    }
    pub(crate) fn insert(&self, db: &Db) -> anyhow::Result<()> {
        warn!("Inserting test row");
        let res = db.conn.execute("INSERT INTO test(pes_id) values (?1)", params![self.pes_id,])?;
        warn!("Result of insert: {:?}", res);
        Ok(())
    }
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self { pes_id: row.get(0)? })
    }
}

impl TransactionRow {
    pub(crate) fn insert(&self, db: &Db) -> anyhow::Result<()> {
        db.conn.execute(
            "INSERT INTO transactions(peer_id, address, is_forwarded, signer_id, receiver_id) values (?1,?2,?3,?4,?5)",
            params![
                self.address.to_string(),
                self.peer_id,
                self.is_forwarded,
                self.signer_id,
                self.receiver_id
            ],
        )?;
        Ok(())
    }
}
