const fetch = require('node-fetch')
const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const nearApi = require('near-api-js')


const { loadJsonToBigquery_holder } = require('./bigquery')
const {StatsApi} = require('./api/stats')

let ERCtokenList = new Map()
let TIME_THRESHOLD = 0
let FILE_READY = false

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
  console.log('update every 3 hour')
  while (true) {
        while (true) {
          try {
            await getERCtokenAsset()
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

        while (FILE_READY) {
          try {
              loadTokenTables()
              break
          } catch (e) {
              console.error('error to submit to holder table: ')
              console.error(e)
              console.error('sleeping for 1 min')
              await sleep(60000);
              continue;
          }
      }

        await sleep(3*60*60000);
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
  let file = tokenList.map(JSON.stringify).join('\n')
  storeData(file, 'tokenList')

  let holderList = Array(tokenList.length)
  for(let i=0;i<tokenList.length;i++) {
    let accountId = await nearRpc.callViewMethod('factory.bridge.near', 'get_bridge_token_account_id', {address: tokenList[i].address.slice(2)})
    holderList[i] = await new StatsApi().getTokenHolders(accountId)
  }

  for(let i=0; i< holderList.length;i++) {
    for(let j=0; j< holderList[i].length;j++) {
      let tokenHolderBalance = await nearRpc.callViewMethod(tokenList[i].address.slice(2) + '.factory.bridge.near', 'ft_balance_of', {account_id: holderList[i][j].holder})
      let decimal = Math.pow(10, tokenList[i].decimals).toString()
      holderList[i][j].symbol = tokenList[i].symbol
      holderList[i][j].timestamp =  moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
      holderList[i][j].balance = new BN(tokenHolderBalance).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000
    }
  }
  
  let ERC20tokenHolder = []
  
  for(let i=0; i<holderList.length;i++) {
    let file = holderList[i].map(JSON.stringify).join('\n')
    storeData(file, path.join(__dirname, 'holderHistory', tokenList[i].symbol))
    ERC20tokenHolder = ERC20tokenHolder.concat(holderList[i])
  }

  let ercFile = ERC20tokenHolder.map(JSON.stringify).join('\n')
  storeData(ercFile, path.join(__dirname, 'holderHistory', 'erc_20_token_holder'))

  FILE_READY = true
}

// load tables
 const loadTokenTables = () => {
   ERCtokenList.forEach((value) => loadJsonToBigquery_holder(`./holderHistory/${value.symbol}`, value.symbol))
 }

main() 