const fetch = require('node-fetch')
const pTh = require('p-throttle')
const throttle = pTh({limit: 100, interval: 60*1000})
const tFetch = throttle(fetch)

const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const nearApi = require('near-api-js')

const tokenListData = require('./tokenList.json')
const tokenList = tokenListData.token

const { loadJsonToBigquery_aurora } = require('./bigquery')
const {queryAuroraTotalTransactionsNumbe} = require('./indexer/index')

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
  console.log('near update every 3 hour')
  while (true) {
        while (true) {
          try {
            await getAccountAmountFromNear()
            // await getGeneralInfo()
            console.log('near nep amount and price updated')
            break
          } catch (e) {
            console.error('error to get near amount: ')
            console.error(e)
            console.error('retrying in 1 min')
            await sleep(60000);
            continue;
        }
      }

      while (false && fs.existsSync('./assetHistory/nep_balance')) {
        try {
            loadJsonToBigquery_aurora('./assetHistory/nep_balance', 'nep_balance')
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

// near account amount

const nearRpcUrl='https://rpc.mainnet.internal.near.org'
const nearRpc = new nearApi.providers.JsonRpcProvider(nearRpcUrl)

nearRpc.callViewMethod = async function (contractName, methodName, args) {
  const account = new nearApi.Account({ provider: this });
  return await account.viewFunction(contractName, methodName, args);
};

async function getAccountAmountFromNear() {
  let NEPList = tokenList.map(t =>({symbol: t.symbol}))

  let timestamp = moment(new Date()).format("YYYY-MM-DD HH:mm:ss")

  for(let i=0; i<tokenList.length; i++) {
    let accountId = tokenList[i].address.slice(2) + '.factory.bridge.near'
    let balance = await nearRpc.callViewMethod(accountId, 'ft_balance_of', {account_id: 'aurora'})
    let decimal = Math.pow(10, Number(tokenList[i].decimals)).toString()
    balance = new BN(balance).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000
    console.log(tokenList[i].symbol, balance)
    NEPList[i].timestamp = timestamp
    NEPList[i].balance = balance
    NEPList[i].priceTime = moment(new Date()).format('DD-MM-YYYY')
  }
  await getPrice(NEPList)
  let file = NEPList.map(JSON.stringify).join('\n')

  storeData(file,path.join(__dirname, 'assetHistory', 'nep_balance'))
}

let tokenMapData = require('./tokenMap')
const tokenMap = tokenMapData.list

async function getPrice(array) {
  for (let i=0; i<array.length;i++) {
    let token = tokenMap.filter((token) => token.symbol === array[i].symbol.toLowerCase())
    let price
    if(token.length>0) {
      let id = token[0].id
      price = await getPriceFromCoingecko(id, array[i].priceTime)
      console.log(id, price)
    } else {
      console.log(array[i].symbol)
      price = 0
    }
    array[i] = {...array[i], price: Number(price)}
  }
  return array
}

async function getPriceFromCoingecko(token, date) {

  let response = await tFetch(`https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`, {
    headers: {
      Accept: "application/json"
    }
  })
  if (response.ok) {
    let res = await response.json()
    
    if(res.market_data){
      return res.market_data.current_price.usd.toFixed(5)
    }else {
      console.log(`https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`)
      console.log(res)
      return null
    }
    
  }
  return 
}

async function getGeneralInfo() {
  let transactionNumber = await queryAuroraTotalTransactionsNumbe()
  // let auroraTotalSupply = await nearRpc.callViewMethod('aurora', 'ft_total_supply', {})
  let auroraAmountETH = await nearRpc.callViewMethod('aurora', 'ft_total_supply_eth', {})
  // auroraTotalSupply = new BN(auroraTotalSupply).mul(new BN('10000')).div(new BN('1000000000000000000000000')).toNumber()/10000
  // console.log(auroraTotalSupply)
  console.log(auroraAmountETH)
}
main()