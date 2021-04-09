const fetch = require('node-fetch')
const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const nearApi = require('near-api-js')

const { loadJsonToBigquery } = require('./bigquery')

let ERCtokenList = new Map()
let tokenMapData = require('./tokenMap')
const tokenMap = tokenMapData.list

const ETH_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f'
const API_KEY = 'JGGYBCHQWMQ9TIU2QVSKI2V1AA43SNSVEW'

let ERC_TOKEN_ASSET = null
let ERC_TOKEN_DEPOSIT = null
let ERC_TOKEN_WITHDRAW = null
let TIME_THRESHOLD = 0

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
                if(ERC_TOKEN_DEPOSIT){
                  storeData(ERC_TOKEN_DEPOSIT,path.join(__dirname, 'assetHistory', `deposit_${TIME_THRESHOLD}`))
                }
                if(ERC_TOKEN_WITHDRAW) {
                  storeData(ERC_TOKEN_WITHDRAW, path.join(__dirname, 'assetHistory', `withdraw_${TIME_THRESHOLD}`))
                }
                if(ERC_TOKEN_ASSET) {
                  storeData(ERC_TOKEN_ASSET,path.join(__dirname, 'assetHistory', `asset_${TIME_THRESHOLD}`))
                }
                console.log("asset updated")
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

        while ( ERC_TOKEN_ASSET && fs.existsSync(`./assetHistory/asset_${TIME_THRESHOLD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/asset_${TIME_THRESHOLD}`, 'asset')
              break
          } catch (e) {
              console.error('error to submit deposit to table: ')
              console.error(e)
              console.error('sleeping for 1 min')
              await sleep(60000);
              continue;
          }
        }

        while ( ERC_TOKEN_DEPOSIT && fs.existsSync(`./assetHistory/deposit_${TIME_THRESHOLD}`)) {
            try {
                loadJsonToBigquery(`./assetHistory/deposit_${TIME_THRESHOLD}`, 'deposit')
                break
            } catch (e) {
                console.error('error to submit deposit to table: ')
                console.error(e)
                console.error('sleeping for 1 min')
                await sleep(60000);
                continue;
            }
        }

        while ( ERC_TOKEN_WITHDRAW && fs.existsSync(`./assetHistory/withdraw_${TIME_THRESHOLD}`)) {
          try {
              loadJsonToBigquery(`./assetHistory/withdraw_${TIME_THRESHOLD}`, 'withdrawl')
              break
          } catch (e) {
              console.error('error to submit to table: ')
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

        await sleep(3*60*60000);
  }
}

async function getERCtokenAsset() {

  let ERCtokenDeposit, ERCtokenWithdrawl, ERCtokenAsset

  let response = await fetch(`https://api.etherscan.io/api?module=account&action=tokentx&address=${ETH_ADDRESS}&startblock=0&endblock=999999999&sort=asc&apikey=${API_KEY}`, {
    headers: {
      Accept: "application/json"
    }
  })

  if (response.ok) {
    let res = await response.json()

    res = res.result.filter((tx) => Number(tx.timeStamp) > TIME_THRESHOLD)
    let deposit = res.filter((tx) => tx.to === ETH_ADDRESS)
    let withdrawl = res.filter((tx) => tx.from === ETH_ADDRESS)

    if(res.length > 0) {
      aggregateTokenMap(res)

      ERCtokenAsset = getAmountList(res)

      if(deposit.length > 0) {
        ERCtokenDeposit = getAmountList(deposit)
      }
      if(withdrawl.length > 0) {
        ERCtokenWithdrawl = getAmountList(withdrawl)
      }

      TIME_THRESHOLD = Number(res[res.length -1].timeStamp)
    }
  }

  if (ERCtokenAsset) {
    ERCtokenAsset = await getPrice(ERCtokenAsset)
  }
  
  if (ERCtokenDeposit) {
    ERCtokenDeposit = await getPrice(ERCtokenDeposit)
  }

  if (ERCtokenWithdrawl) {
    ERCtokenWithdrawl = await getPrice(ERCtokenWithdrawl)
  }


  ERC_TOKEN_ASSET = ERCtokenAsset ? ERCtokenAsset.map(JSON.stringify).join('\n') : null
  ERC_TOKEN_DEPOSIT = ERCtokenDeposit ? ERCtokenDeposit.map(JSON.stringify).join('\n') : null
  ERC_TOKEN_WITHDRAW = ERCtokenWithdrawl ? ERCtokenWithdrawl.map(JSON.stringify).join('\n') : null
}

async function getPriceFromCoingecko(token, date) {

  let response = await fetch(`https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`, {
    headers: {
      Accept: "application/json"
    }
  })
  if (response.ok) {
    let res = await response.json()
    return res.market_data.current_price.usd.toFixed(5)
  }
  return 
}

const getAmountList = (array) =>(
  array.map( (tx) =>{
  let decimal = Math.pow(10, tx.tokenDecimal).toString()
  return {
    symbol: tx.tokenSymbol, 
    amount: new BN(tx.value).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000,
    timestamp: moment.unix(tx.timeStamp).format("YYYY-MM-DD HH:mm:ss"),
    priceTime: moment.unix(tx.timeStamp).format('DD-MM-YYYY')
  }}
  )
)

const getPrice = async (array) => {
  for (let i=0; i<array.length;i++) {
    let token = tokenMap.filter((token) => token.symbol === array[i].symbol.toLowerCase())
    let id = token[0].id
    let price = await getPriceFromCoingecko(id, array[i].priceTime)
    array[i] = {...array[i], price: Number(price)}
  }
  return array
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

//load price 
async function getTokenMap() {
  let res = await fetch(`https://api.coingecko.com/api/v3/coins/list`, {
    headers: {
      Accept: "application/json"
    }
  })
  if(res.ok) {
    let obj = {}
    let list = await res.json()
    obj.list = list
    let json = JSON.stringify(obj)
    storeData(json, 'tokenMap.json')
  }
}

main() 