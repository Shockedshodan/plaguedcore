use chrono::{DateTime, Utc};
use db::{Db, TransactionRow};
use near_primitives::transaction::{SignedTransaction};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::Path;
use std::{env, fmt};
use near_primitives::types::{AccountId};
use near_primitives::network::PeerId;


mod db;
mod json_helper;

pub enum TransactionOrigin {
    ClientAdapter,
    Client,
    SendTxAsync,
    SendTxCommit,
}
impl fmt::Display for TransactionOrigin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TransactionOrigin::ClientAdapter => write!(f, "ClientAdapter"),
            TransactionOrigin::Client => write!(f, "Client"),
            TransactionOrigin::SendTxAsync => write!(f, "SendTxAsync"),
            TransactionOrigin::SendTxCommit => write!(f, "SendTxCommit"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CensoredTransaction {
    transaction: SignedTransaction,
    blacklisted_id: AccountId,
    where_censored: String,
    timestamp: DateTime<Utc>,
}




pub fn plague_watch(transaction: SignedTransaction, peer_id: PeerId, socket_address: Option<SocketAddr>, is_forwarded: u8) -> bool {
    let db = Db::open(Path::new("plague.db")).unwrap();
    let row = TransactionRow {
        address: socket_address.unwrap(),
        peer_id: peer_id.to_string(),
        is_forwarded,
        signer_id: transaction.transaction.signer_id.to_string(),
        receiver_id: transaction.transaction.receiver_id.to_string(),
    };
    row.insert(&db).unwrap();
    true
}


pub fn plague_touch(transaction: SignedTransaction, origin: TransactionOrigin) -> bool {
    if !check_if_env_exists() {
        return false;
    }
    let is_blacklisted = check_blacklisted(transaction.clone());
    if is_blacklisted.0 {
        let censored_transaction = CensoredTransaction {
            transaction,
            blacklisted_id: is_blacklisted.1.unwrap(),
            where_censored: origin.to_string(),
            timestamp: Utc::now(),
        };
        json_helper::deal_with_json(&censored_transaction, &origin.to_string());
        return true;
    }
    false
}

fn check_blacklisted(transaction: SignedTransaction) -> (bool, Option<AccountId>) {
    let blacklist = get_env_blacklist();
    let receiver_id = transaction.transaction.receiver_id;
    let signer_id = transaction.transaction.signer_id;
    if blacklist.contains(&receiver_id) {
        return (true, Some(receiver_id));
    }
    if blacklist.contains(&signer_id) {
        return (true, Some(signer_id));
    }
    (false, None)
}

fn get_env_blacklist() -> Vec<AccountId> {
    let env_var = env::var("BLACKLIST").unwrap_or_else(|_| String::from(""));
    let temp_account_id_vector: Vec<String> = env_var.split(',').map(|s| s.to_owned()).collect();
    let mut account_ids: Vec<AccountId> = Vec::new();
    for account_id_string in temp_account_id_vector {
        let account_id: AccountId = account_id_string.parse().unwrap();
        account_ids.push(account_id);
    }
    account_ids
}

fn check_if_env_exists() -> bool {
    env::var("BLACKLIST").is_ok()
}
