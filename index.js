const fetch = require('node-fetch')
const pTh = require('p-throttle')
const throttle = pTh({limit: 100, interval: 60*1000})
const tFetch = throttle(fetch)

const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')

const { loadJsonToBigquery } = require('./bigquery')

let ERCtokenList = new Map()
let tokenMapData = require('./tokenMap')
const tokenMap = tokenMapData.list

const ETH_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f'
const API_KEY = 'JGGYBCHQWMQ9TIU2QVSKI2V1AA43SNSVEW'

let ERC_TOKEN_ASSET = null
let ERC_TOKEN_DEPOSIT = null
let ERC_TOKEN_WITHDRAW = null
let TIME_THRESHOLD = 1621421827

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
              console.log("start to upload asset")
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
                console.log("start to upload deposit")
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
              console.log("start to upload withdrawl")
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

        await sleep(3*60*60000);
  }
}

async function getERCtokenAsset() {

  let ERCtokenDeposit, ERCtokenWithdrawl, ERCtokenAsset

  let response = await tFetch(`https://api.etherscan.io/api?module=account&action=tokentx&address=${ETH_ADDRESS}&startblock=0&endblock=999999999&sort=asc&apikey=${API_KEY}`, {
    headers: {
      Accept: "application/json"
    }
  })

  if (response.ok) {
    let res = await response.json()

    res = res.result.filter((tx) => Number(tx.timeStamp) > TIME_THRESHOLD)
    console.log("result length", res.length)

    if(res.length > 0) {
      aggregateTokenMap(res)

      ERCtokenAsset = getAmountList(res)

      TIME_THRESHOLD = Number(res[res.length -1].timeStamp)
    }
  }

  if (ERCtokenAsset) {
    ERCtokenAsset = await getPrice(ERCtokenAsset)
  }
  
  if(ERCtokenAsset) {
    ERCtokenDeposit = ERCtokenAsset.filter((tx) => tx.action === 'deposit')  
    ERCtokenWithdrawl = ERCtokenAsset.filter((tx) => tx.action === 'withdraw')
  }
  
  ERC_TOKEN_ASSET = ERCtokenAsset ? ERCtokenAsset.map(JSON.stringify).join('\n') : null
  ERC_TOKEN_DEPOSIT = ERCtokenDeposit ? ERCtokenDeposit.map(JSON.stringify).join('\n') : null
  ERC_TOKEN_WITHDRAW = ERCtokenWithdrawl ? ERCtokenWithdrawl.map(JSON.stringify).join('\n') : null
}

async function getPriceFromCoingecko(token, date) {

  let response = await tFetch(`https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`, {
    headers: {
      Accept: "application/json"
    }
  })
  if (response.ok) {
    let res = await response.json()
    console.log(`https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`)
    if(res.market_data){
      console.log(res.market_data.current_price.usd)
      return res.market_data.current_price.usd.toFixed(5)
    }else {
      console.log(res)
      return null
    }
    
  }
  return 
}

const getAmountList = (array) =>(
  array.map( (tx) =>{
  let decimal = Math.pow(10, tx.tokenDecimal).toString()
  return {
    symbol: tx.tokenSymbol, 
    amount: new BN(tx.value).mul(new BN('1000000000')).div(new BN(decimal)).toNumber()/1000000000,
    timestamp: moment.unix(tx.timeStamp).format("YYYY-MM-DD HH:mm:ss"),
    priceTime: moment.unix(tx.timeStamp).format('DD-MM-YYYY'),
    action: tx.to === ETH_ADDRESS ? 'deposit': 'withdraw'
  }}
  )
)

const getPrice = async (array) => {
  for (let i=0; i<array.length;i++) {
    let token = tokenMap.filter((token) => token.symbol === array[i].symbol.toLowerCase())
    let price
    if(token.length>0) {
      let id = token[0].id
      price = await getPriceFromCoingecko(id, array[i].priceTime)
      console.log(id, price, array[i].priceTime)
    } else {
      console.log(array[i].symbol)
      price = 0
    }
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