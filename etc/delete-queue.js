//  -*- coding: utf-8 -*-
//  delete-queue.js ---
//  created: 2018-04-19 08:43:45
//

const assert = require('assert');
const {Connection} = require('..');

async function rm(name) {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    await rabbit.deleteQueue(name);
    await rabbit.disconnect();
}

assert(process.argv[2], 'usage: node delete-queue.js <queue name>');

rm(process.argv[2]).then(() => console.log('done'));

process.on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at:', p, 'reason:', reason);
});

//
//  delete-queue.js ends here
