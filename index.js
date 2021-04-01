const fetch = require('node-fetch')
const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')
const nearApi = require('near-api-js')

const { loadJsonToBigquery } = require('./bigquery')

const ERCtokenList = [
    { name: "USDT",  address: "0xdac17f958d2ee523a2206206994597c13d831ec7", decimals: 6},
    { name: "UNI",   address: "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", decimals: 18 },
    { name: "LINK",  address: "0x514910771af9ca656af840dff83e8264ecf986ca", decimals: 18 },
    { name: "USDC",  address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", decimals: 6  }, 
    { name: "WBTC",  address: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", decimals: 8  }, 
    { name: "AAVE",  address: "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9", decimals: 18 },
    { name: "CRO",   address: "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b", decimals: 8  }, 
    { name: "FTT",   address: "0x50d1c9771902476076ecfc8b2a83ad6b9355a4c9", decimals: 18 }, 
    { name: "BUSD",  address: "0x4fabb145d64652a948d72533023f6e7a623c7c53", decimals: 18 }, 
    { name: "HT",    address: "0x6f259637dcd74c767781e37bc6133cd6a68aa161", decimals: 18 }, 
    { name: "DAI",   address: "0x6b175474e89094c44da98b954eedeac495271d0f", decimals: 18 },
    { name: "SUSHI", address: "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2", decimals: 18 }, 
    { name: "SNX",   address: "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f", decimals: 18 },
    { name: "GRT",   address: "0xc944e90c64b2c07662a292be6244bdf05cda44a7", decimals: 18 }, 
    { name: "MKR",   address: "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2", decimals: 18 }, 
    { name: "COMP",  address: "0xc00e94cb662c3520282e6f5717214004a7f26888", decimals: 18 },
    { name: "YFI",   address: "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e", decimals: 18 },
    { name: "WETH",  address: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", decimals: 18 }, 
    { name: "HBTC",  address: "0x0316eb71485b0ab14103307bf65a021042c6d380", decimals: 18 }, 
    { name: "1INCH", address: "0x111111111117dc0aa78b770fa6a738034120c302", decimals: 18 }, 
    { name: "MATIC", address: "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0", decimals: 18 },
    { name: "SNT",   address: "0x744d70FDBE2Ba4CF95131626614a1763DF805B9E", decimals: 18 },
    { name: "ALCX",  address: "0xdbdb4d16eda451d0503b854cf79d55697f90c8df", decimals: 18 }
]

const tokenMap = new Map([
    ["USDT",  "tether"],
    ["UNI",   "unicorn-token"],
    ["LINK",  "link"],
    ["USDC",  "usd-coin"], 
    ["WBTC",  "wrapped-bitcoin"], 
    ["AAVE",  "aave"],
    ["CRO",   "crypto-com-chain"], 
    ["FTT",   "freetip"], 
    ["BUSD",  "binance-usd"], 
    ["HT",    "huobi-token"], 
    ["DAI",   "dai"],
    ["SUSHI", "sushi"], 
    ["SNX",   "havven"],
    ["GRT",   "the-graph"], 
    ["MKR",   "maker"], 
    ["COMP",  "compound-coin"],
    ["YFI",   "yearn-finance"],
    ["WETH",  "weth"], 
    ["HBTC",  "huobi-btc"], 
    ["1INCH", "1inch"],
    ["SNT",   "status"],
    ["MATIC", "matic-network"],
    ["ALCX",  "alchemix"] 
])

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
  console.log('update every 12 hour')
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

        await sleep(12*60*60000);
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
    let price = await getPriceFromCoingecko(tokenMap.get(array[i].symbol), array[i].priceTime)
    array[i] = {...array[i], price}
  }
  return array
}

// near account amount

const nearRpcUrl='https://rpc.mainnet.internal.near.org'
const nearRpc = new nearApi.providers.JsonRpcProvider(nearRpcUrl)

nearRpc.callViewMethod = async function (contractName, methodName, args) {
  const account = new nearApi.Account({ provider: this });
  return await account.viewFunction(contractName, methodName, args);
};

async function getAccountAmountFromNear() {
  let accountIdList = ERCtokenList.map((token) => ({
                                                    symbol:token.name, 
                                                    balance: 0,
                                                    timestamp: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}))
  for(let i=0; i<accountIdList.length; i++) {
    let accountId = await nearRpc.callViewMethod('factory.bridge.near', 'get_bridge_token_account_id', {address: ERCtokenList[i].address.slice(2)})
    let balance = await nearRpc.callViewMethod(accountId, 'ft_total_supply', {})
    let decimal = Math.pow(10, ERCtokenList[i].decimals).toString()
    balance = new BN(balance).mul(new BN('10000')).div(new BN(decimal)).toNumber()/10000
    accountIdList[i].balance = balance
  }

  let file = accountIdList.map(JSON.stringify).join('\n')

  storeData(file,path.join(__dirname, 'assetHistory', 'near_balance'))
}


main() 