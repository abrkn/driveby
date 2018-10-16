const assert = require('assert');
const { URL } = require('url');
const safep = require('safep');
const bitcoin = require('bitcoin');
const Promise = require('bluebird');
const redis = require('redis');
Promise.promisifyAll(redis);

function urlToBitcoinOptions(url) {
  return {
    host: url.hostname || 'localhost',
    port: url.port,
    user: url.username || 'user',
    pass: url.password,
  };
}

const { SOURCE_BITCOIND_URL, DESTINATION_BITCOIND_URL, REDIS_URL } = process.env;
const SOURCE_MIN_HEIGHT = +(process.env.SOURCE_MIN_HEIGHT || 543543);

const sourceBitcoinRpc = new bitcoin.Client(urlToBitcoinOptions(new URL(SOURCE_BITCOIND_URL)));
safep.applyTo(sourceBitcoinRpc, 'cmd');

const destBitcoinRpc = new bitcoin.Client(urlToBitcoinOptions(new URL(DESTINATION_BITCOIND_URL)));
safep.applyTo(destBitcoinRpc, 'cmd');

const redisConn = redis.createClient(REDIS_URL);

const main = async () => {
  // const help = await sourceBitcoinRpc.cmdAsync('help');
  // console.log(help);
  // assert();

  const prevHeight = +(await redisConn.getAsync('driveby.height')) || SOURCE_MIN_HEIGHT;
  const { blocks: sourceHeight } = await sourceBitcoinRpc.cmdAsync('getblockchaininfo');

  for (let height = prevHeight + 1; height <= sourceHeight; height++) {
    const blockHash = await sourceBitcoinRpc.cmdAsync('getblockhash', height);
    const blockWithTxs = await sourceBitcoinRpc.cmdAsync('getblock', blockHash, 2);
    const { tx: blockTxs } = blockWithTxs;

    for (const txIndex in blockTxs) {
      if (+txIndex === 0) {
        continue;
      }

      const tx = blockTxs[txIndex];

      const { hex: txHex, hash: txHash } = tx;

      const [error] = await destBitcoinRpc.cmdSafe('sendrawtransaction', txHex);

      if (error) {
        console.error(height, txHash, 'ERROR', error.message);
      } else {
        console.log(height, txHash, 'OK');
      }
    }

    await redisConn.setAsync('driveby.height', height);
  }

  // const help = await sourceBitcoinRpc.cmdAsync('help');
  // console.log(help);
};

main().then(process.exit);
