//  -*- coding: utf-8 -*-
//  hello-consumer.js ---
//  created: 2018-04-18 00:48:36
//

const {
    Connection,
    Consumer
} = require('..');

console.debug = (msg) => console.log(msg);

const QUEUE_SUFFIX = process.env.QUEUE ? `-${process.env.QUEUE}` : '';
const AUTODEL = process.env.AUTODEL === '1';
const EXCLUSIVE = process.env.EXCLUSIVE === '1';
const ACK = process.env.ACK === '1';
const EXCHANGE = process.env.EXCHANGE || 'es_events';

const options = {
    queue: 'jeditest' + QUEUE_SUFFIX,
    durable: false,
    autoDelete: AUTODEL,
    exclusive: EXCLUSIVE,
    ack: ACK,
    keys: ['trip.*', 'route.*'],
};

console.log('## queue options:');
console.log(JSON.stringify(options, null, '  '));

async function connect() {
    const gotMilk = (key, payload, msg) => {
        console.log('====================================================================================');
        console.log(JSON.stringify(payload, null, '  '));
        // console.log(`got ${key}`);
        if (!ACK) {
            return;
        }
        setTimeout(async () => {
            if (Math.random() < 0.3) {
                console.log(`   -> ACK`, payload);
                consumer.channel.ack(msg);
            } else {
                console.log(`   -> NAK`, payload);
                consumer.channel.nack(msg);
            }
        }, 2000);
    };

    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    const consumer = new Consumer(rabbit, EXCHANGE, options);
    consumer.on('msg', gotMilk);
    await consumer.init();
};

connect().then(() => console.log('connected'));

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});

//
//  hello-consumer.js ends here
