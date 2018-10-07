/*
 * File name:    producer.js
 * File Created: 2018-10-07T23:04:77
 * Company:      Vinka
 */

'use strict';

const assert = require('assert');
const {EventEmitter} = require('events');
const {sleep, marshal} = require('./util');

class RabbitProducer extends EventEmitter {
    constructor(rabbit, exchangeName, options={}) {
        super();
        this.log = options.logger || console;
        this.rabbit = rabbit;
        this.channel = null;
        this.exchangeName = exchangeName;
    }

    async init() {
        this.rabbit.on('connected', () => this._open());
        this.rabbit.on('disconnected', () => this._close());

        // already connected -> just open channels
        if (this.rabbit.connection) {
            await this._open();
        }
    }

    async send(key, msg, options={}) {
        assert(this.channel, `channel not open for exchange "${this.exchangeName}" when sending`);

        if (options.delay) {
            await sleep(options.delay);
        }

        this.log.debug(`sending with key "${key}" to exhange "${this.exchangeName}"`);

        await this.channel.publish(this.exchangeName, key, marshal(msg));
    }

    async _open() {
        const onChannelError = err => {
            this.log.error(`channel error on "${name}" ` + err);
            this._reconnect();
        }
        const onChannelClose = msg => {
            // If rabbit server goes down,
            // the only way we notice the outage is that channel is closed
            this.rabbit.reconnect();
            this.log.info(`${this.exchangeName} channel closed: ` + msg);
        };

        const ch = await this.rabbit.connection.createChannel();
        ch.assertExchange(this.exchangeName, 'topic', {durable: false});
        ch.on('error', onChannelError);
        ch.on('close', onChannelClose);
        this.channel = ch;
        this.log.info(`ready to send events to exchange "${this.exchangeName}"`);
    }

    async _close() {
        const ch = this.channel;

        if (!ch) {
            return;
        }

        this.channel = null;

        try {
            await ch.close();
        } catch (e) {
            this.log.error(`channel failed to close: ` + e);
        }
    }

    async _reconnect() {
        try {
            await this._close();
            await this._open();
        } catch (e) {
            this.log.error('rabbit producer failed to reconnect: ' + e);
            setTimeout(this._reconnect.bind(this), 3000);
        }
    }
}

module.exports = RabbitProducer;
