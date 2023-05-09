CREATE TABLE IF NOT EXISTS transactions (
    date TEXT NOT NULL DEFAULT (datetime('now')),   
    peer_id TEXT NOT NULL,
    address TEXT NOT NULL,
    is_forwarded INTEGER NOT NULL,  
    signer_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL,          
)