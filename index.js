const Web3 = require('web3')
const fetch = require('node-fetch')
const fs = require('fs')
const path = require('path')
const moment = require('moment')
const BN = require('bn.js')

const { loadJsonToBigqueryPrice, loadJsonToBigquerySupply} = require('./bigquery')

const ERCtokenList = [
    { "name": "USDT",  "address": "0xdac17f958d2ee523a2206206994597c13d831ec7","id": "tether", "decimals": 6  },
    { "name": "UNI",   "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984","id": "unicorn-token","decimals": 18 },
    { "name": "LINK",  "address": "0x514910771af9ca656af840dff83e8264ecf986ca","id": "link", "decimals": 18 },
    { "name": "USDC",  "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","id": "usd-coin", "decimals": 6  }, 
    { "name": "WBTC",  "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599","id": "wrapped-bitcoin", "decimals": 8  }, 
    { "name": "AAVE",  "address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9","id": "aave",  "decimals": 18 },
    { "name": "CRO",   "address": "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b","id": "crypto-com-chain", "decimals": 8  }, 
    { "name": "FTT",   "address": "0x50d1c9771902476076ecfc8b2a83ad6b9355a4c9","id": "freetip", "decimals": 18 }, 
    { "name": "BUSD",  "address": "0x4fabb145d64652a948d72533023f6e7a623c7c53","id": "binance-usd", "decimals": 18 }, 
    { "name": "HT",    "address": "0x6f259637dcd74c767781e37bc6133cd6a68aa161","id": "huobi-token", "decimals": 18 }, 
    { "name": "DAI",   "address": "0x6b175474e89094c44da98b954eedeac495271d0f","id": "dai",  "decimals": 18 },
    { "name": "SUSHI", "address": "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2","id": "sushi", "decimals": 18 }, 
    { "name": "SNX",   "address": "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f","id": "havven", "decimals": 18 },
    { "name": "GRT",   "address": "0xc944e90c64b2c07662a292be6244bdf05cda44a7","id": "the-graph", "decimals": 18 }, 
    { "name": "MKR",   "address": "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2","id": "maker", "decimals": 18 }, 
    { "name": "COMP",  "address": "0xc00e94cb662c3520282e6f5717214004a7f26888","id": "compound-coin", "decimals": 18 },
    { "name": "YFI",   "address": "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e","id": "yearn-finance", "decimals": 18 },
    { "name": "WETH",  "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "id": "weth", "decimals": 18 }, 
    { "name": "HBTC",  "address": "0x0316eb71485b0ab14103307bf65a021042c6d380","id": "huobi-btc", "decimals": 18 }, 
    { "name": "1INCH", "address": "0x111111111117dc0aa78b770fa6a738034120c302","id": "1inch", "decimals": 18 } 
]

let ERC_TOKEN_BALANCE = null
let ERC_TOKEN_PRICE = null

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function main() {
  console.log('update every 15 min')
  while (true) {
        while (true) {
            try {
                await getTotalAmountFromEtherem()
                await getPriceFromCoingecko()
                storeData(ERC_TOKEN_BALANCE,path.join(__dirname,'supply'))
                storeData(ERC_TOKEN_PRICE, path.join(__dirname, 'price'))
                break
            } catch (e) {
                console.error('error to get total supply: ')
                console.error(e)
                console.error('retrying in 1 min')
                await sleep(60000);
                continue;
            }
        }

        // while ( fs.existsSync('./supply') && fs.existsSync('./price')) {
        //     try {
        //         loadJsonToBigquerySupply('./supply')
        //         loadJsonToBigqueryPrice('./price')
        //         console.log('ERC_TOKEN_BALANCE updated')
        //         console.log('ERC_TOKEN_PRICE updated')
        //         break
        //     } catch (e) {
        //         console.error('error to submit to table: ')
        //         console.error(e)
        //         console.error('sleeping for 1 min')
        //         await sleep(60000);
        //         continue;
        //     }
        // }

        await sleep(900000);
  }
}

async function getTotalAmountFromEtherem() {
  let url = 'https://mainnet.infura.io/v3/5c58abcce1b14e4ab56e2ef4f801d86c'

  const web3 = new Web3(url)

  const ERC20TransferABI = [
    {
      constant: true,
      inputs: [
      ],
      name: "totalSupply",
      outputs: [
        {
          name: "balance",
          type: "uint256",
        },
      ],
      payable: false,
      stateMutability: "view",
      type: "function",
    },
  ]

  let ERCTokenBalance = ERCtokenList.map((token) => ({name: token.name, supply: 0, timestamp: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}))

  let tokenFeed = new Array(ERCtokenList)
  
  for(let i=0; i<ERCtokenList.length; i++) {
    tokenFeed[i] = new web3.eth.Contract(ERC20TransferABI, ERCtokenList[i].address)
  }

  for(let i=0; i<ERCtokenList.length; i++) {
    try {
      let res = await tokenFeed[i].methods.totalSupply().call()
      ERCTokenBalance[i].supply = new BN(res).div(new BN(Math.pow(10, ERCtokenList[i].decimals)))
    } catch(err) {
        console.log("An error occured", err)
        return
    }
  }

  ERC_TOKEN_BALANCE = ERCTokenBalance.map(JSON.stringify).join('\n');
}

async function getPriceFromCoingecko() {

  let ERCTokenPrice = ERCtokenList.map((token) => ({name: token.name, price: 0, timestamp: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}))

  for(let i=0;i<ERCtokenList.length;i++){
    let response = await fetch(`https://api.coingecko.com/api/v3/simple/price?ids=${ERCtokenList[i].id}&vs_currencies=usd`, {
    headers: {
      Accept: "application/json"
    }
  })
    if(response.ok){
      let usdPrice = await response.json()
      ERCTokenPrice[i].price = usdPrice[ERCtokenList[i].id].usd
    }
  }

  ERC_TOKEN_PRICE = ERCTokenPrice.map(JSON.stringify).join('\n');
}

const storeData = (data, path) => {
  try {
    fs.writeFileSync(path, data)
  } catch (err) {
    console.error(err)
  }
}

main() 