CREATE TABLE IF NOT EXISTS transactions (
    date TEXT NOT NULL DEFAULT (datetime('now')),   
    peer_id TEXT NOT NULL,
    address TEXT NOT NULL,
    is_forwarded INTEGER NOT NULL,  
    signer_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL      
);

CREATE TABLE IF NOT EXISTS forwarded_transactions (
    date TEXT NOT NULL DEFAULT (datetime('now')),   
    validator TEXT NOT NULL,
    balance_nonce INTEGER NOT NULL,  
    tx_hash TEXT NOT NULL,
    signer_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL      
);

CREATE TABLE IF NOT EXISTS test (
    date TEXT NOT NULL DEFAULT (datetime('now')),   
    pes_id TEXT NOT NULL    
);