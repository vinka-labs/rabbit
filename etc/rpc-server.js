/*
 * File name:    rpc-server.js
 * File Created: 2018-09-27T16:46:18
 * Company:      Vinka
 */

const {
    Connection,
    RpcServer
} = require('..');

const options = {
    queue: 'my-rpc-queue',
    prefetchCount: 3,
};
const sleep = ms => new Promise(r => setTimeout(r, ms));
const sleeprand = () => sleep(Math.random() * 1000 + 1000);

const evalService = async (msg) => {
    console.log('====================================================================================');
    console.log(`processing "${msg}"...`);
    await sleeprand();
    try {
        return eval(msg);
    } catch (e) {
        return {_error: e.message};
    }
}

async function connect() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    const consumer = new RpcServer(rabbit, evalService, options);
    await consumer.init()
};

connect().then(() => {
    console.log('done');
});

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});
