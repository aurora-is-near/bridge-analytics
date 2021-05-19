// Import the Google Cloud client libraries
const {BigQuery} = require('@google-cloud/bigquery');
const fs = require('fs')

// Instantiate client
const bigquery = new BigQuery();

const datasetId = 'bridge_erc20_token'

async function loadJsonToBigquery(file, table) {
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    location: 'US',
  };
  const [job] = await bigquery
    .dataset(datasetId)
    .table(table)
    .load(file, metadata)

  console.log(`Job ${job.id} completed.`)

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  } else {
    fs.unlink(file,(err) => {
      if (err) {
          throw err;
      }
  
      console.log("File is deleted.", file);
  })
  }
}

const holderDatasetId = 'bridge_token_holder'

async function loadJsonToBigquery_holder(file, table) {
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    location: 'US',
  };
  const [job] = await bigquery
    .dataset(holderDatasetId)
    .table(table)
    .load(file, metadata)

  console.log(`Job ${job.id} completed.`)

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  } else {
    fs.unlink(file,(err) => {
      if (err) {
          throw err;
      }
  
      console.log("File is deleted.", file);
  })
  }
}

// aurora project
const auroraDataset = 'aurora'
async function loadJsonToBigquery_aurora(file, table) {
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    location: 'US',
  };
  const [job] = await bigquery
    .dataset(auroraDataset)
    .table(table)
    .load(file, metadata)

  console.log(`Job ${job.id} completed.`)

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  } else {
    fs.unlink(file,(err) => {
      if (err) {
          throw err;
      }
  
      console.log("File is deleted.", file);
  })
  }
}

exports.loadJsonToBigquery = loadJsonToBigquery
exports.loadJsonToBigquery_holder = loadJsonToBigquery_holder
exports.loadJsonToBigquery_aurora = loadJsonToBigquery_aurora