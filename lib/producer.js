/*
 * File name:    producer.js
 * File Created: 2018-10-07T23:04:77
 * Company:      Vinka
 */

'use strict';

const assert = require('assert');
const {EventEmitter} = require('events');
const {sleep, marshal, addChannelMaintenance, openChannel} = require('./util');

class RabbitProducer extends EventEmitter {
    constructor(rabbit, exchangeName, options={}) {
        super();
        this.log = options.logger || console;
        if (!this.log.silly) {
            // make sure we have silly level available even without winston logger
            this.log.silly = console.debug;
        }
        this.rabbit = rabbit;
        this.channel = null;
        this.exchangeName = exchangeName;

        addChannelMaintenance(this);
    }

    async send(key, msg, options={}) {
        assert(this.channel, `channel not open for exchange "${this.exchangeName}" when sending`);

        if (options.delay) {
            await sleep(options.delay);
        }

        this.log.silly(`sending with key "${key}" to exhange "${this.exchangeName}"`);

        await this.channel.publish(this.exchangeName, key, marshal(msg));
    }

    async _open() {
        const ch = await openChannel(this);
        ch.assertExchange(this.exchangeName, 'topic', {durable: false});
        this.channel = ch;
        this.log.info(`ready to send events to exchange "${this.exchangeName}"`);
    }
}

module.exports = RabbitProducer;
