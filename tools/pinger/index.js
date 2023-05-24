const { connect, transactions, keyStores } = require("near-api-js");
const sqlite3 = require('sqlite3').verbose();
const fs = require("fs");
const path = require("path");
const homedir = require("os").homedir();

const CREDENTIALS_DIR = ".near-credentials";
// NOTE: replace "example" with your accountId
const RECEIVER_ID = "censormehardermommy.testnet";
const SIGNER = "plaguenet.testnet"

const credentialsPath = path.join(homedir, CREDENTIALS_DIR);
const keyStore = new keyStores.UnencryptedFileSystemKeyStore(credentialsPath);

const init_string = "CREATE TABLE IF NOT EXISTS transactions (date TEXT NOT NULL DEFAULT (datetime('now')), tx_hash TEXT NOT NULL, balance_nonce INTEGER NOT NULL)";
const insert_string = "INSERT INTO transactions(tx_hash, balance_nonce) values (?1,?2)";

const storeToDb = (hash, balance_nonce) => {
    const db = new sqlite3.Database('pinger_tx');
    db.serialize(()=>{
        db.run(init_string);
        const stmt = db.prepare(insert_string);
        stmt.run(hash, balance_nonce);
        stmt.finalize();
    })
}
const config = {
    keyStore,
    networkId: "testnet",
    // nodeUrl: "https://rpc.testnet.near.org",
    nodeUrl: "localhost:3030",
};


let count = 1;


const ping = async () => {
    const near = await connect({ ...config, keyStore });
    const account = await near.account(SIGNER);
    const tx_res = await account.sendMoney(RECEIVER_ID, `${count}`)
    storeToDb(tx_res.transaction.hash, count);
    count++;
    console.log(count);
}



setInterval(ping, 5000)
