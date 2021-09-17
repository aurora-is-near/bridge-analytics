async function getPriceFromCoingecko(token, date) {
  let response = await fetch(
    `https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`,
    {
      headers: {
        Accept: 'application/json',
      },
    }
  );
  if (response.ok) {
    let res = await response.json();
    console.log(
      `https://api.coingecko.com/api/v3/coins/${token}/history?date=${date}&localization=false`
    );
    if (res.market_data) {
      console.log(res.market_data.current_price.usd);
      return res.market_data.current_price.usd.toFixed(5);
    } else {
      console.log(res);
      return null;
    }
  }
  return;
}

const getPrice = async (array) => {
  for (let i = 0; i < array.length; i++) {
    let token = tokenMap.filter(
      (token) => token.symbol === array[i].symbol.toLowerCase()
    );
    let price;
    if (token.length > 0) {
      let id = token[0].id;
      price = await getPriceFromCoingecko(id, array[i].priceTime);
      console.log(id, price, array[i].priceTime);
    } else {
      console.log(array[i].symbol);
      price = 0;
    }
    array[i] = { ...array[i], price: Number(price) };
  }
  return array;
};

const fs = require('fs');
let tokenMapData = require('./tokenMap');
const tokenMap = tokenMapData.list;
const fetch = require('node-fetch');

const storeData = (data, path) => {
  try {
    fs.writeFileSync(path, data);
  } catch (err) {
    console.error(err);
  }
};

let ori_data = require('./no-price.json');

async function main() {
  let new_data = await getPrice(ori_data);
  let file = new_data.map(JSON.stringify).join('\n');
  storeData(file, 'fix-price-3');
}

main();
