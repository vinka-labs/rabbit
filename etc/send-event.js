/*
 * File name:    send-event.js
 * File Created: 2020-09-24T10:46:88
 * Company:      Vinka
 */

'use strict';

var amqp = require('amqplib/callback_api');

const fs = require('fs');

const {Connection, Producer} = require('..');

const EXCHANGE = process.env.EXCHANGE || 'jediex';
const KEY = process.env.KEY;

if (!KEY) {
    throw 'must provide KEY';
}

if (!process.env.MSG_DATA) {
    throw 'must provide MSG_DATA';
}

const msg = JSON.parse(fs.readFileSync(process.env.MSG_DATA).toString());

console.debug = console.log;

async function operate() {
    // create rabbit connection
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();

    // create producer
    const producer = new Producer(rabbit, EXCHANGE, {logger: console});
    await producer.init();

    // send event
    const key = process.env.KEY || 'anonymous.info';
    producer.send(key, msg, {delay: 100});
    console.log(`sent ${JSON.stringify(msg)} to ${key} in exchange ${EXCHANGE}`);

    // close connection
    setTimeout(async () => {
        console.log('========================================================== END');
        await rabbit.connection.close();
    }, 1000);
}

operate().then(() => console.log('done'));

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});
