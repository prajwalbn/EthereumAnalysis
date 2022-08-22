The goal of this Analysis is to apply the Big Data Processing techniques to analyze the full set of transactions that have occurred on the Ethereum network; from the first transactions in August 2015 till June 2019

Dataset overview
Ethereum is a blockchain-based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mine their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralized, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

Whilst you would normally need a CLI tool such as GETH to access the Ethereum blockchain, recent tools allow scraping all block/transactions and dump these to csv's to be processed in bulk; notably Ethereum-ETL. These dumps are uploaded daily into a repository on Google BigQuery. We have used this source as the dataset for this analysis

Dataset Schema - blocks
number: The block number

hash: Hash of the block

miner: The address of the beneficiary to whom the mining rewards were given

difficulty: Integer of the difficulty for this block

size: The size of this block in bytes

gas_limit: The maximum gas allowed in this block

gas_used: The total used gas by all transactions in this block

timestamp: The timestamp for when the block was collated

transaction_count: The number of transactions in the block

Dataset Schema - transactions
block_number: Block number where this transaction was in

from_address: Address of the sender

to_address: Address of the receiver. null when it is a contract creation transaction

value: Value transferred in Wei (the smallest denomination of ether)

gas: Gas provided by the sender

gas_price : Gas price provided by the sender in Wei

block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

Dataset Schema - contracts
address: Address of the contract

is_erc20: Whether this contract is an ERC20 contract

is_erc721: Whether this contract is an ERC721 contract

block_number: Block number where this contract was created

Analysis Performed--
Part A -Time Analysis
Part B -Top Ten Most Popular Services
Part C - Top Ten Most Active Miners
Part D - Data exploration
