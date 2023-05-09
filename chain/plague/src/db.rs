//! Database connection and struct representing rows in the data tables.

use std::path::Path;
use rusqlite::{params, Connection, Row};
use chrono::{DateTime, Utc};
use near_primitives::transaction::{SignedTransaction, Action};
use serde::{Deserialize, Serialize};
use std::{env, fmt};
use near_crypto::{PublicKey, Signature};
use near_primitives::types::{Nonce, AccountId};
use near_primitives::hash::CryptoHash;
use std::net::{SocketAddr};

use crate::TransactionOrigin;


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
        let conn = Connection::open(path)?;
        let init_sql = include_str!("init.sql");
        conn.execute_batch(init_sql)?;
        Ok(Self::new(conn))
    }
}
pub struct ParsedTransactionRow {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub signature: Signature,
    pub nonce: Nonce,
    pub receiver_id: AccountId,
    pub block_hash: [u8; 32],
    pub actions: Vec<Action>,
    pub origin: TransactionOrigin,
}

pub struct TransactionRow {
    pub address: SocketAddr,
    pub peer_id: String,
    pub is_forwarded: u8, 
    pub signer_id: String,
    pub receiver_id: String,
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
