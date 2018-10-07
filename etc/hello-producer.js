//  -*- coding: utf-8 -*-
//  hello-producer.js ---
//  created: 2018-04-18 01:02:12
//

var amqp = require('amqplib/callback_api');

const {Connection, Producer} = require('..');

const NUM = parseInt(process.env.NUM_EXCHANGES) || 1;
const EX_PREFIX = process.env.EX_PREFIX || 'jediex';

console.debug = console.log;

async function operate() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();

    const producers = [];
    for (let i = 0; i < NUM; i += 1) {
        const exname = `${EX_PREFIX}-${i + 1}`;
        const producer = new Producer(rabbit, exname, {logger: console});
        await producer.init();
        producers.push(producer);
    }

    const key = process.env.KEY || 'anonymous.info';

    for (let i = 0; i < NUM; i += 1) {
        const msg = {msg: `${process.env.MSG || 'hello'} ${Math.floor(Math.random() * 100)}`};
        const exname = `${EX_PREFIX}-${i + 1}`;
        producers[i].send(key, msg, {delay: 100});
        console.log(`sent ${JSON.stringify(msg)} to ${key} in exchange ${exname}`);
    }

    setTimeout(async () => {
        console.log('========================================================== END');
        await rabbit.connection.close();
    }, 1000);
}

operate().then(() => console.log('done'));

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});

//
//  hello-producer.js ends here
