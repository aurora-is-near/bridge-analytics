const { ExplorerApi } = require('./index')

class StatsApi extends ExplorerApi {
  async getTokenHolders(accountId) {
    return await this.call("get-bridge-token-holders", [accountId]);    
  }
}

exports.StatsApi = StatsApi