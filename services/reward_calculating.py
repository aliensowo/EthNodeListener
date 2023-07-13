from variables import REWARDS, BURN_FEE_BLOCK
from web3.eth import BlockData
from web3 import Web3
from query_dir.query import create_uncle


def get_miner_reward(block_number: int) -> int:
    for miner_reward, block_range in REWARDS.items():
        if block_number in range(*block_range):
            return miner_reward


def get_block_reward(w3: Web3, block: BlockData):
    block_reward = 0
    txn_fee = 0
    burned_fee = 0
    block_inclusion = 0
    block_number = block.number
    # Static block reward
    miner_reward = get_miner_reward(block_number=block_number)
    # burned fee
    if block_number >= BURN_FEE_BLOCK:
        burned_fee = float(w3.fromWei(block.get("baseFeePerGas", 1) * block.gasUsed, "ether"))
    # txn fee
    for txn in block.transactions:
        txn_fee += float(w3.fromWei(txn.gasPrice * txn.gas, "ether"))
    block_reward += txn_fee
    block_reward -= burned_fee
    block_reward += miner_reward
    block_reward += block_inclusion
    return float(block_reward)


def get_uncle_inclusion_reward(w3: Web3, block_number: int, uncles):
    miner_reward = get_miner_reward(block_number=block_number)
    for uncle_index in range(len(uncles)):
        uncle_reward = 0
        uncle_block = w3.eth.get_uncle_by_block(block_number, uncle_index)
        uncle_block_number = int(uncle_block.number, base=16)
        uncle_reward += (uncle_block_number + 8 - block_number) * miner_reward / 8
        timestamp_uncle = int(uncle_block.timestamp, base=16)
        uncle_hash = uncle_block.hash
        create_uncle(
            block_number=block_number,
            uncle_number=uncle_block_number,
            uncle_index=uncle_index,
            reward=uncle_reward,
            timestamp=timestamp_uncle,
            uncle_hash=uncle_hash,
        )
        print(f"Success write DataBase Uncle Block. BlockNumber: {block_number} | UncleNumber: {uncle_block_number}")
