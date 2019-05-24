/*
 * File name:    rpc-client.js
 * File Created: 2018-09-28T11:52:97
 * Company:      Vinka
 */

const {
    Connection,
    RpcClient
} = require('..');

const options = {};

const op = () => {
    const num = () => Math.floor(Math.random() * 100);
    return `${num()} + ${num()}`;
};

const NUM = parseInt(process.env.NUM || '1');

async function operate() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();

    const rpc = new RpcClient(rabbit, 'my-rpc-queue', {logger: console});
    await rpc.init();
    const key = process.env.KEY || 'anonymous.info';

    const ops = [...Array(NUM).keys()].map(() => process.env.MSG || op());
    console.log(ops);

    const proms = ops.map(async msg => {
        return (async () => {
            try {
                console.log(`sending ${msg}...`);
                const result = await rpc.exec('eval', [msg], {timeout: 40000});
                // const result = await rpc.rpc('eval', msg);
                console.log(`sent ${msg} to ${key} -> ${result} expected: ${result === eval(msg)}`);
            } catch (e) {
                console.log(`sent ${msg} to ${key} -> failed: ${e.message}`);
            }
        })();
    });

    await Promise.all(proms);
    // for (let i = 0; i < ops.length; i += 1) {
    //     const msg = ops[i];
    //     try {
    //         const result = await producers[i].rpc(msg);
    //         console.log(`sent ${msg} to ${key} -> ${result} expected: ${result === eval(msg)}`);
    //     } catch (e) {
    //         console.log(`sent ${msg} to ${key} -> failed: ${e.message}`);
    //     }
    // }

    setTimeout(async () => {
        await rabbit.connection.close();
    }, 1000);
}

operate().then(() => console.log('done'));

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});
