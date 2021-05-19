const Pool = require('pg').Pool
const pool = new Pool({
  user: 'explorer',
  host: '35.240.76.233',
  database: 'mainnet_explorer',
  password: 'H+848CGRqtqZ4vVd',
  port: 5432,
});

const queryBridgeTokenHolders = async (bridgeTokenContractId) => {
  let query = `SELECT 
      DISTINCT transactions.signer_account_id AS holder
    FROM receipts 
    JOIN action_receipt_actions ON receipts.receipt_id = action_receipt_actions.receipt_id  
    JOIN transactions ON transactions.transaction_hash = receipts.originated_from_transaction_hash 
    WHERE receipts.receiver_account_id = '${bridgeTokenContractId}'
    AND action_receipt_actions.action_kind = 'FUNCTION_CALL' 
    AND action_receipt_actions.args->> 'method_name' = 'mint'`
  return await new Promise(function(resolve, reject) {
    pool.query(query, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
    })
  }) 
}

const queryBridgeMintTransactionAction = async (timestamp) => {
  return await new Promise(function(resolve, reject) {
    pool.query(`SELECT
    receipts.receiver_account_id AS symbol_token,  
    action_receipt_actions.args ->> 'args_base64' AS serialized_amount, 
    TO_TIMESTAMP(receipts.included_in_block_timestamp / 1000000000) as timestamp,
    originated_from_transaction_hash as transaction_hash
   FROM receipts
   JOIN action_receipt_actions ON receipts.receipt_id = action_receipt_actions.receipt_id 
   WHERE
     receipts.predecessor_account_id = 'factory.bridge.near'
     AND action_receipt_actions.args ->> 'method_name' = 'mint'
     AND TO_TIMESTAMP(receipts.included_in_block_timestamp/1000000000) > '${timestamp}'
   ORDER BY included_in_block_timestamp DESC`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
    })
  }) 
}

const queryBridgeDepositTransaction = async (timestamp) => {
  return await new Promise(function(resolve, reject) {
    pool.query(`SELECT
    receipts.predecessor_account_id as account_id,
    TO_TIMESTAMP(receipts.included_in_block_timestamp / 1000000000) as timestamp,
    receipts.originated_from_transaction_hash as transaction_hash 
  FROM receipts
  JOIN action_receipt_actions ON receipts.receipt_id = action_receipt_actions.receipt_id 
  WHERE
    receipts.receiver_account_id = 'factory.bridge.near'
    AND action_receipt_actions.args ->> 'method_name' = 'deposit'
    AND TO_TIMESTAMP(receipts.included_in_block_timestamp/1000000000) > '${timestamp}'
  ORDER BY included_in_block_timestamp DESC`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
    })
  }) 
}

const queryBridgeFinishWithdrawTransaction = async (timestamp) => {
  return await new Promise(function(resolve, reject) {
    pool.query(`SELECT
    receipts.predecessor_account_id as account_id,
    TO_TIMESTAMP(receipts.included_in_block_timestamp / 1000000000) as timestamp,
    receipts.originated_from_transaction_hash as transaction_hash
   FROM receipts
   JOIN action_receipt_actions ON receipts.receipt_id = action_receipt_actions.receipt_id 
   WHERE
     receipts.receiver_account_id = 'factory.bridge.near'
     AND action_receipt_actions.args ->> 'method_name' = 'finish_withdraw'
     AND TO_TIMESTAMP(receipts.included_in_block_timestamp/1000000000) > '${timestamp}'
   ORDER BY included_in_block_timestamp DESC`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
    })
  }) 
}

const queryBridgeWithdrawTransactionAction = async (timestamp) => {
  return await new Promise(function(resolve, reject) {
    pool.query(`SELECT
    receipts.predecessor_account_id as predecessor,
    receipts.receiver_account_id as symbol_token,
    action_receipt_actions.args ->> 'args_base64' as serialized_amount, 
    TO_TIMESTAMP(receipts.included_in_block_timestamp / 1000000000) as timestamp,
    receipts.originated_from_transaction_hash as transaction_hash,
    action_receipt_actions.args ->> 'method_name' as method
  FROM receipts
  JOIN action_receipt_actions ON receipts.receipt_id = action_receipt_actions.receipt_id 
  WHERE receipts.receiver_account_id like '%factory.bridge.near'
  AND action_receipt_actions.args ->> 'method_name' IN ('withdraw', 'ft_transfer')
  AND TO_TIMESTAMP(receipts.included_in_block_timestamp/1000000000) > '${timestamp}'
  ORDER BY included_in_block_timestamp DESC`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
    })
  }) 
}

const queryAuroraTotalTransactionsNumbe = async () => {
  return await new Promise(function(resolve, reject) {
    pool.query(`select count(*)
    from transactions
    where signer_account_id = 'aurora' or receiver_account_id = 'aurora'`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows[0].count);
    })
  }) 
}

exports.queryBridgeTokenHolders = queryBridgeTokenHolders
exports.queryBridgeMintTransactionAction = queryBridgeMintTransactionAction
exports.queryBridgeDepositTransaction = queryBridgeDepositTransaction
exports.queryBridgeFinishWithdrawTransaction = queryBridgeFinishWithdrawTransaction
exports.queryBridgeWithdrawTransactionAction = queryBridgeWithdrawTransactionAction
exports.queryAuroraTotalTransactionsNumbe = queryAuroraTotalTransactionsNumbe