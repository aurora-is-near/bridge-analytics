const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const hexy = require('hexy')

const {
    queryBridgeDepositTransaction,
    queryBridgeMintTransactionAction,
    queryBridgeWithdrawTransactionAction,
    queryBridgeFinishWithdrawTransaction
} = require('./indexer/index')
const {loadJsonToBigquery} = require('./bigquery')

const tokenListData = require('./tokenList.json')
const tokenList = tokenListData.token

let DEPOSIT_TIME_THREAD = '2021-03-15 00:00:00'
let FINISH_WITHDRAW_TIME_THREAD = '2021-03-15 00:00:00'
let MINT_TIME_THREAD = '2021-03-15 00:00:00'
let WITHDRAW_TIME_THREAD = '2021-03-15 00:00:00'

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const storeData = (data, path) => {
try {
  fs.writeFileSync(path, data)
} catch (err) {
  console.error(err)
}
}

async function main() {
  console.log('near deposit update every 3 hr')
  while (true) {
        while (true) {
            try {
                await getNearDepositAndFinishWithDraw()
                console.log("near deposit and withdraw updated")
                await getMintAndWithdrawAction()
                console.log("near mint and withdraw updated")
                break
            } catch (e) {
                console.error('error to get near deposit and finish withdraw: ')
                console.error(e)
                console.error('retrying in 1 min')
                await sleep(60000);
                continue;
            }
        }

        while ( fs.existsSync(`./assetHistory/near_deposit_${DEPOSIT_TIME_THREAD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/near_deposit_${DEPOSIT_TIME_THREAD}`, 'near_deposit_transaction')
              break
          } catch (e) {
              console.error('error to submit deposit transaction to table: ')
              console.error(e)
              await sleep(60000);
              continue;
          }
        }

        while ( fs.existsSync(`./assetHistory/near_finish_withdraw_${FINISH_WITHDRAW_TIME_THREAD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/near_finish_withdraw_${FINISH_WITHDRAW_TIME_THREAD}`, 'near_finish_withdraw_transaction')
              break
          } catch (e) {
              console.error('error to submit withdraw transaction to table: ')
              console.error(e)
              await sleep(60000);
              continue;
          }
        }

        while ( fs.existsSync(`./assetHistory/near_mint_${MINT_TIME_THREAD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/near_mint_${MINT_TIME_THREAD}`, 'near_mint')
              break
          } catch (e) {
              console.error('error to submit mint to table: ')
              console.error(e)
              await sleep(60000);
              continue;
          }
        }

        while ( fs.existsSync(`./assetHistory/near_withdraw_${WITHDRAW_TIME_THREAD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/near_withdraw_${WITHDRAW_TIME_THREAD}`, 'near_withdraw')
              break
          } catch (e) {
              console.error('error to submit withdraw to table: ')
              console.error(e)
              await sleep(60000);
              continue;
          }
        }
        
        await sleep(3*60*60000);
  }
}

async function getNearDepositAndFinishWithDraw() {
  let deposit = await queryBridgeDepositTransaction(DEPOSIT_TIME_THREAD)
  let withdraw = await queryBridgeFinishWithdrawTransaction(FINISH_WITHDRAW_TIME_THREAD)
  
  DEPOSIT_TIME_THREAD = moment(deposit[0].timestamp).add(1,'seconds').format("YYYY-MM-DD HH:mm:ss")
  FINISH_WITHDRAW_TIME_THREAD = moment(withdraw[0].timestamp).add(1,'seconds').format("YYYY-MM-DD HH:mm:ss")
  console.log("current deposit timestamp: ", DEPOSIT_TIME_THREAD)
  console.log("current finish withdraw timestamp", FINISH_WITHDRAW_TIME_THREAD)

  let depositFile = deposit.map(d => ({...d, timestamp: moment(d.timestamp).format("YYYY-MM-DD HH:mm:ss")})).map(JSON.stringify).join('\n')
  storeData(depositFile, path.join(__dirname, 'assetHistory', `near_deposit_${DEPOSIT_TIME_THREAD}`))
  let withdrawFile = withdraw.map(d => ({...d, timestamp: moment(d.timestamp).format("YYYY-MM-DD HH:mm:ss")})).map(JSON.stringify).join('\n')
  storeData(withdrawFile, path.join(__dirname, 'assetHistory', `near_finish_withdraw_${FINISH_WITHDRAW_TIME_THREAD}`))

}

async function getMintAndWithdrawAction() {
  let deposit = await queryBridgeMintTransactionAction(MINT_TIME_THREAD)
  let withdraw = await queryBridgeWithdrawTransactionAction(WITHDRAW_TIME_THREAD)

  MINT_TIME_THREAD = moment(deposit[0].timestamp).add(1,'seconds').format("YYYY-MM-DD HH:mm:ss")
  WITHDRAW_TIME_THREAD = moment(withdraw[0].timestamp).add(1,'seconds').format("YYYY-MM-DD HH:mm:ss")
  console.log("current mint timestamp: ", MINT_TIME_THREAD)
  console.log("current withdraw and transfer timestamp", WITHDRAW_TIME_THREAD)

  deposit = deposit.map((d) => {
    let indexOf = d.symbol_token.indexOf('.')
    let token = tokenList.filter((t) => t.address.slice(2) === d.symbol_token.slice(0, indexOf))
    let args = deserializeArgs(d.serialized_amount)
    let decimal = Math.pow(10, token[0].decimals).toString()
    return ({
      transaction_hash: d.transaction_hash,
      symbol: token ? token[0].symbol : d.symbol_token,
      timestamp: moment(d.timestamp).format("YYYY-MM-DD HH:mm:ss"),
      amount: args ? new BN(args.amount).mul(new BN('1000000000')).div(new BN(decimal)).toNumber()/1000000000: null,
      holder: args ? args.account_id : null
    })
  })

  withdraw = withdraw.map((d) => {
    let indexOf = d.symbol_token.indexOf('.')
    let token = tokenList.filter((t) => t.address.slice(2) === d.symbol_token.slice(0, indexOf))
    let args = deserializeArgs(d.serialized_amount)
    let decimal = Math.pow(10, token[0].decimals).toString()
    let amount
    if(args) amount = args.amount.length < 10  ? Number(args.amount) :  new BN(args.amount).mul(new BN('1000000000')).div(new BN(decimal)).toNumber()/1000000000
    return ({
      predecessor: d.predecessor, 
      transaction_hash: d.transaction_hash,
      symbol: token ? token[0].symbol : d.symbol_token,
      timestamp: moment(d.timestamp).format("YYYY-MM-DD HH:mm:ss"),
      amount,
      receiver: args ? args.receiver_id: null,
      method: d.method,
      
    })
  })
  
  let depositFile = deposit.map(JSON.stringify).join('\n')
  storeData(depositFile, path.join(__dirname, 'assetHistory', `near_mint_${MINT_TIME_THREAD}`))
  let withdrawFile = withdraw.map(JSON.stringify).join('\n')
  storeData(withdrawFile, path.join(__dirname, 'assetHistory', `near_withdraw_${WITHDRAW_TIME_THREAD}`))

}

const deserializeArgs = (args) => {
  const decodedArgs = Buffer.from(args, "base64");
  let prettyArgs;
  try {
    const parsedJSONArgs = JSON.parse(decodedArgs.toString());
    prettyArgs = JSON.stringify(parsedJSONArgs, null, 2);
  } catch {
    prettyArgs = hexy(decodedArgs, { format: "twos" });
  }
  return JSON.parse(prettyArgs)
};

main()