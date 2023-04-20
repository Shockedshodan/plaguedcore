## Prerequisites
 -  Rust
 -  git
 -  NodeJS
 -  NearCLI

 ### MacOS
 ```brew install cmake protobuf llvm awscli```
 ```curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash```
 ```nvm install --lts```

 ### Linux
``` apt update```
``` apt install -y git binutils-dev libcurl4-openssl-dev zlib1g-dev libdw-dev libiberty-dev cmake gcc g++ python docker.io protobuf-compiler libssl-dev pkg-config clang llvm cargo awscli```
```curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash```
 ```nvm install --lts```




If you want to run local node:
1. Deal with prereqs
2. ```git clone git@github.com:Shockedshodan/plaguedcore.git```
3. ```cd plaguedcore``
4. ```git checkout plagued-1.33.0-rc.1```
5. ```make neard```
6. ```./target/release/neard --home ~/.near init --chain-id localnet```
7. Set an a blacklist - ```export BLACKLIST=alice.test.near```
8.  ```./target/release/neard --home ~/.near run```
9. ```npm install -g near-cli```
10. ```export NEAR_ENV=localnet```
11. ```near create-account alice.test.near --masterAccount test.near```


What we did is very simple: run the node, install nearcli, try to create an a censored account. After that we will get a freshly created JSON in the root folder of the project.