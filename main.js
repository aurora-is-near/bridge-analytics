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
const tokenList = require('./tokenList.json')

let DEPOSIT_TIME_THREAD = '2021-01-11 00:00:00'
let FINISH_WITHDRAW_TIME_THREAD = '2021-01-11 00:00:00'
let MINT_TIME_THREAD = '2021-01-11 00:00:00'
let WITHDRAW_TIME_THREAD = '2021-01-11 00:00:00'

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
  console.log('near deposit update every 3 hour')
  while (true) {
        while (true) {
            try {
                await getNearDepositAndFinishWithDraw()
                
                console.log("near deposit and withdraw updated")
                console.log("current timestamp: " + TIME_THRESHOLD)
                break
            } catch (e) {
                console.error('error to get total asset: ')
                console.error(e)
                console.error('retrying in 1 min')
                await sleep(60000);
                continue;
            }
        }

        await sleep(3*60*60000);
  }
}

async function getNearDepositAndFinishWithDraw() {
  let deposit = await queryBridgeDepositTransaction('2021-01-11 00:00:00')
  let withdraw = await queryBridgeFinishWithdrawTransaction('2021-01-11 00:00:00')
  console.log(deposit)
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
  return prettyArgs
};