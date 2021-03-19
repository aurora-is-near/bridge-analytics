// Import the Google Cloud client libraries
const {BigQuery} = require('@google-cloud/bigquery');

// Instantiate client
const bigquery = new BigQuery();

const datasetId = 'bridge_erc20_token'
const tableId = 'total_supply'
const priceTableId = 'price'

async function loadJsonToBigquerySupply(file) {
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    location: 'US',
  };
  const [job] = await bigquery
    .dataset(datasetId)
    .table(tableId)
    .load(file, metadata)

  console.log(`Job ${job.id} completed.`)

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }

}

async function loadJsonToBigqueryPrice(file) {
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    location: 'US',
  };
  const [job] = await bigquery
    .dataset(datasetId)
    .table(priceTableId)
    .load(file, metadata)

  console.log(`Job ${job.id} completed.`)

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }

}

exports.loadJsonToBigquerySupply = loadJsonToBigquerySupply
exports.loadJsonToBigqueryPrice = loadJsonToBigqueryPrice