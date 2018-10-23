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
const sleeprand = () => sleep(Math.random() * 1000 + 5000);

const operations = {
    eval: async (msg) => {
        console.log('====================================================================================');
        console.log(`processing "${msg}"... ${typeof msg}`);
        await sleeprand();
        try {
            const result = eval(msg);
            console.log('result: ' + msg);
            return result;
        } catch (e) {
            return {_error: e.message};
        }
    },
}

async function connect() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    const server = new RpcServer(rabbit, operations, options);
    await server.init()
};

connect().then(() => {
    console.log('done');
});

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});
