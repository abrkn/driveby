const assert = require('assert');
const { URL } = require('url');
const safep = require('safep');
const bitcoin = require('bitcoin');
const Promise = require('bluebird');
const redis = require('redis');
const pMap = require('p-map');

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
const TX_CONCURRENCY = 10;

const sourceBitcoinRpc = new bitcoin.Client(urlToBitcoinOptions(new URL(SOURCE_BITCOIND_URL)));
safep.applyTo(sourceBitcoinRpc, 'cmd');

const destBitcoinRpc = new bitcoin.Client(urlToBitcoinOptions(new URL(DESTINATION_BITCOIND_URL)));
safep.applyTo(destBitcoinRpc, 'cmd');

const redisConn = redis.createClient(REDIS_URL);

const main = async () => {
  const prevHeight = +(await redisConn.getAsync('driveby.height')) || SOURCE_MIN_HEIGHT;
  const { blocks: sourceHeight } = await sourceBitcoinRpc.cmdAsync('getblockchaininfo');

  const submitTx = async (height, tx, index) => {
    if (+index === 0) {
      return;
    }

    const { hex: txHex, hash: txHash } = tx;

    const [error] = await destBitcoinRpc.cmdSafe('sendrawtransaction', txHex);

    if (error) {
      console.error(height, txHash, 'ERROR', error.message);
      return;
    }

    console.log(height, txHash, 'OK');
  };

  for (let height = prevHeight + 1; height <= sourceHeight; height++) {
    const blockHash = await sourceBitcoinRpc.cmdAsync('getblockhash', height);
    const blockWithTxs = await sourceBitcoinRpc.cmdAsync('getblock', blockHash, 2);
    const { tx: blockTxs } = blockWithTxs;

    await pMap(blockTxs, (tx, index) => submitTx(height, tx, index), { concurrency: TX_CONCURRENCY });

    await redisConn.setAsync('driveby.height', height);
  }
};

main().then(process.exit);
