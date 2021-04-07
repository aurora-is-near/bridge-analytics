const autobahn = require('autobahn')

class ExplorerApi {
  static awaitingOnSession = [];

  static subscriptions = {};

  static wamp;

  dataSource = "INDEXER_BACKEND";
  nearNetwork = {
    "name": "mainnet", 
    "explorerLink": "https://explorer.near.org/", 
    "aliases": ["explorer.near.org", "explorer.mainnet.near.org", "explorer.nearprotocol.com", "explorer.mainnet.nearprotocol.com"], 
    "lockupAccountIdSuffix": "lockup.near", 
    "nearWalletProfilePrefix": "https://wallet.near.org/profile"
  };

  constructor() {
    if (ExplorerApi.wamp === undefined) {
      let wampNearExplorerUrl = "wss://near-explorer-wamp.onrender.com/ws";
      
      ExplorerApi.wamp = new autobahn.Connection({
        url: wampNearExplorerUrl,
        realm: "near-explorer",
        retry_if_unreachable: true,
        max_retries: Number.MAX_SAFE_INTEGER,
        max_retry_delay: 10,
      });
    }
  }

  // Establish and handle concurrent requests to establish WAMP connection.
  static getWampSession() {
    return new Promise(
      ( resolve, reject ) => {
        if (ExplorerApi.wamp.transport.info.type === "websocket") {
          // The connection is open/opening
          if (ExplorerApi.wamp.session && ExplorerApi.wamp.session.isOpen) {
            // Resolve the established session as it is ready
            resolve(ExplorerApi.wamp.session);
          } else {
            // Push the promise resolvers on a queue
            ExplorerApi.awaitingOnSession.push({ resolve, reject });
          }
        } else {
          // Establish new session
          ExplorerApi.awaitingOnSession.push({ resolve, reject });

          ExplorerApi.wamp.onopen = (session) => {
            Object.entries(
              ExplorerApi.subscriptions
            ).forEach(([topic, [handler, options]]) =>
              session.subscribe(topic, handler, options)
            );
            while (ExplorerApi.awaitingOnSession.length > 0) {
              ExplorerApi.awaitingOnSession.pop().resolve(session);
            }
          };

          ExplorerApi.wamp.onclose = (reason) => {
            while (ExplorerApi.awaitingOnSession.length > 0) {
              ExplorerApi.awaitingOnSession.pop().reject(reason);
            }
            return false;
          };

          ExplorerApi.wamp.open();
        }
      }
    );
  }

  async subscribe(topic, handler, options){
    topic = `com.nearprotocol.${this.nearNetwork.name}.explorer.${topic}`;
    ExplorerApi.subscriptions[topic] = [handler, options];
    const session = await ExplorerApi.getWampSession();
    return await session.subscribe(topic, handler, options);
  }

  async call(procedure, args, kwargs, options) {
    procedure = `com.nearprotocol.${this.nearNetwork.name}.explorer.${procedure}`;
    const session = await ExplorerApi.getWampSession();
    return (await session.call(procedure, args, kwargs, options));
  }
}

exports.ExplorerApi = ExplorerApi