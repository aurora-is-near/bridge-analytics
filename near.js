const fetch = require('node-fetch')
const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const nearApi = require('near-api-js')


const { loadJsonToBigquery_holder, loadJsonToBigquery } = require('./bigquery')
const {queryBridgeTokenHolders} = require('./indexer/index')

let ERCtokenList = new Map()
let TIME_THRESHOLD = 0

const ETH_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f'
const API_KEY = 'JGGYBCHQWMQ9TIU2QVSKI2V1AA43SNSVEW'

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
  console.log('near update day')
  while (true) {
        while (true) {
          try {
            await getERCtokenAsset()
            console.log('current timestamp:', TIME_THRESHOLD)
            if(ERCtokenList.size > 0){
              await getTokenHoldersDist()
            }
            console.log('near token holder updated')
            break
          } catch (e) {
            console.error('error to get near amount: ')
            console.error(e)
            console.error('retrying in 1 min')
            await sleep(60000);
            continue;
        }
        }

        while (fs.existsSync('./holderHistory/erc_20_token_holder')) {
          try {
              console.log('submit holder to table')
              loadJsonToBigquery_holder('./holderHistory/erc_20_token_holder', 'erc_20_token_holder')
              break
          } catch (e) {
              console.error('error to submit to holder table: ')
              console.error(e)
              console.error('sleeping for 1 min')
              await sleep(60000);
              continue;
          }
        }

        while (true) {
          try {
            await getAccountAmountFromNear()
            console.log('near amount updated')
            break
          } catch (e) {
            console.error('error to get near amount: ')
            console.error(e)
            console.error('retrying in 1 min')
            await sleep(60000);
            continue;
        }
        }

        while (fs.existsSync('./assetHistory/near_balance')) {
          try {
              console.log('submit near balance to table')
              loadJsonToBigquery('./assetHistory/near_balance', 'near_balance')
              break
          } catch (e) {
              console.error('error to submit to volume table: ')
              console.error(e)
              console.error('sleeping for 1 min')
              await sleep(60000);
              continue;
          }
        }
        
        await sleep(24*60*60000);
  }
}

async function getERCtokenAsset() {
  let response = await fetch(`https://api.etherscan.io/api?module=account&action=tokentx&address=${ETH_ADDRESS}&startblock=0&endblock=999999999&sort=asc&apikey=${API_KEY}`, {
    headers: {
      Accept: "application/json"
    }
  })

  if (response.ok) {
    let res = await response.json()

    res = res.result.filter((tx) => Number(tx.timeStamp) > TIME_THRESHOLD)

    if(res.length > 0) {
      aggregateTokenMap(res)
      console.log("get token map")
      TIME_THRESHOLD = Number(res[res.length -1].timeStamp)
    }
  }
}

const aggregateTokenMap = (array) => {
  for(let i=0; i<array.length;i++) {
    if(!ERCtokenList.get(array[i].tokenSymbol)){
      ERCtokenList.set(array[i].tokenSymbol, { symbol: array[i].tokenSymbol,  address: array[i].contractAddress, decimals: array[i].tokenDecimal})
    }
  }
}
// near account amount

const nearRpcUrl='https://rpc.mainnet.internal.near.org'
const nearRpc = new nearApi.providers.JsonRpcProvider(nearRpcUrl)

nearRpc.callViewMethod = async function (contractName, methodName, args) {
  const account = new nearApi.Account({ provider: this });
  return await account.viewFunction(contractName, methodName, args);
};

// get token holder

async function getTokenHoldersDist() {
  let tokenList = []
  ERCtokenList.forEach((value) => tokenList.push(value))
  let token = {token: tokenList}
  let file = JSON.stringify(token)
  storeData(file, 'tokenList.json')
  console.log('store token list')

  let holderList = Array(tokenList.length)
  for(let i=0;i<tokenList.length;i++) {
    let accountId = await nearRpc.callViewMethod('factory.bridge.near', 'get_bridge_token_account_id', {address: tokenList[i].address.slice(2)})
    holderList[i] = await queryBridgeTokenHolders(accountId)
  }

  let timestamp = moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
  for(let i=0; i< holderList.length;i++) {
    for(let j=0; j< holderList[i].length;j++) {
      let tokenHolderBalance = await nearRpc.callViewMethod(tokenList[i].address.slice(2) + '.factory.bridge.near', 'ft_balance_of', {account_id: holderList[i][j].holder})
      let decimal = Math.pow(10, tokenList[i].decimals).toString()
      holderList[i][j].symbol = tokenList[i].symbol
      holderList[i][j].timestamp =  timestamp
      holderList[i][j].balance = new BN(tokenHolderBalance).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000
    }
  }

  console.log('holder list finish')
  let ercFile = holderList.map((l)=>l.map(JSON.stringify).join('\n')).join('\n')
  storeData(ercFile, path.join(__dirname, 'holderHistory', 'erc_20_token_holder'))
}

async function getAccountAmountFromNear() {
  let accountIdList = []
  const getList = (value) => {
    accountIdList.push({symbol:value.symbol, balance: 0,timestamp: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")})
  } 
  ERCtokenList.forEach(getList);

  for(let i=0; i<accountIdList.length; i++) {
    let accountId = await nearRpc.callViewMethod('factory.bridge.near', 'get_bridge_token_account_id', {address: ERCtokenList.get(accountIdList[i].symbol).address.slice(2)})

    let balance = await nearRpc.callViewMethod(accountId, 'ft_total_supply', {})

    let decimal = Math.pow(10, ERCtokenList.get(accountIdList[i].symbol).decimals).toString()
    balance = new BN(balance).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000
    accountIdList[i].balance = balance
  }

  let file = accountIdList.map(JSON.stringify).join('\n')

  storeData(file,path.join(__dirname, 'assetHistory', 'near_balance'))
}

main() 