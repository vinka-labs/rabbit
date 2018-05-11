//  -*- coding: utf-8 -*-
//  index.js ---
//  created: 2017-03-09 21:24:25
//

'use strict';

const {EventEmitter} = require('events');
const assert = require('assert');
const amqp = require('amqplib/channel_api');

const sleep = ms => new Promise(r => setTimeout(r, ms));
const marshal = obj => new Buffer(JSON.stringify(obj));
const unmarshal = data => JSON.parse(data.toString());

class RabbitConnection extends EventEmitter {

    /**
     * @param url {String} - the URL of the RabbitMQ server.
     * @param [options] {Object} - options.
     * @param [options.logger=console] {Object} - logger.
     */
    constructor(url, options={}) {
        super();
        assert(url, 'rabbit URL must be given');
        this.url = url;
        this.connection = null;
        this.log = options.logger || console;
    }

    async connect() {
        this.log.debug('connecting rabbit...');
        return amqp.connect(this.url)
        .then(conn => {
            this.log.info(`rabbit connected to ${this.url}`);
            this.connection = conn;
            conn.on('error', this._onConnectionError.bind(this));
            this.emit('connected');
        });
    }

    async disconnect() {
        if (this.connection) {
            const connection = this.connection;
            this.connection = null;
            this.log.debug('closing connection...');
            try {
                connection.close();
                await sleep(500);
            } catch (e) {
                this.log.error('error closing connection: ' + e);
            }
            this.emit('disconnected');
        }
    }

    async deleteQueue(name) {
        const ch = await this.connection.createChannel();
        await ch.deleteQueue(name);
        await ch.close();
    }

    async reconnect() {
        return this._reconnect();
    }

    async _onConnectionError(err) {
        this.log.error('amqp connection error: ' + err);
        this.connection = null;
        await sleep(3000);
        if (!this.connection) {
            this._reconnect();
        }
    }

    async _reconnect() {
        await this.disconnect();
        try {
            await this.connect();
        } catch (err) {
            this.log.error('unable to reconnect to amqp: ' + err);
            setTimeout(this._reconnect.bind(this), 3000);
        }
    }
}

class RabbitConsumer extends EventEmitter {

    /**
     * @param rabbit {Rabbit} - rabbit connection.
     * @param excange {String} - name of the exchange.
     * @param [options] {Object} - options.
     * @param [options.logger=console] {Object} - logger.
     * @param [options.queue=''] {String} - name of the queue. (rabbit will auto-generate if not given)
     * @param [options.durable=false] {Boolean} - whether the queue should be durable or not.
     * @param [options.exclusive=false] {Boolean} - whether the queue should be exclusive or shared with consumers.
     * @param [options.autoDelete=false] {Boolean} - if true, queue will be deleted when all consumers disappear.
     * @param [options.messageTtl] - TTL for messages (in milliseconds).
     * @param [options.ack=false] {Boolean} - whether the applicaiton needs to explicitly ack messages.
     * @param [options.keys=['#'] String[] - routing keys.
     */
    constructor(rabbit, exchange, options={}) {
        super();

        assert(rabbit);

        this.rabbit = rabbit;
        this.log = options.logger || console;
        this.channel = null;
        this.exchangeName = exchange;
        this.queueName = options.queue || '';
        this.queueOptions = {
            durable: !!options.durable,
            exclusive: !!options.exclusive,
            autoDelete: !!options.autoDelete,
        };
        if (options.messageTtl) {
            this.queueOptions.messageTtl = options.messageTtl;
        }
        this.consumeOptions = {
            noAck: !options.ack,
        };
        this.routingKeys = options.keys || ['#']; // # means everything
    }

    async listen() {
        this.rabbit.on('connected', () => {
            this._reconnect();
        });

        this.rabbit.on('disconnect', () => {
            this._close();
        });

        // already connected -> open queue
        if (this.rabbit.connection) {
            this._reconnect();
        }
    }

    async _msgReceived(msg) {
        try {
            const jsonContent = unmarshal(msg.content);
            this.emit('msg', msg.fields.routingKey, jsonContent, msg);
        } catch (e) {
            this.log.error('unable to parse received message: ' + e);
        }
    }

    async _open() {
        const conn = this.rabbit.connection;

        if (!conn) {
            throw Error(`no rabbit connection`);
        }

        const onChannelError = err => {
            this.log.error(`${this.exchangeName} channel error: ` + err);
            this.log.error(err);
            setTimeout(this._reconnect.bind(this), 3000);
        };

        const onChannelClose = msg => {
            this.log.info(`${this.exchangeName} channel closed: ` + msg);
        };

        // open channel
        const channel = await conn.createChannel();
        channel.on('error', onChannelError);
        channel.on('close', onChannelClose);
        const q = await channel.assertQueue(this.queueName , this.queueOptions);
        this.routingKeys.forEach(key => {
            channel.bindQueue(q.queue, this.exchangeName, key);
        });

        channel.consume(q.queue, this._msgReceived.bind(this), this.consumeOptions);
        this.channel = channel;
        this.log.debug(`${this.exchangeName} waiting for events...`);
    }

    async _close() {
        if (this.channel) {
            const channel = this.channel;
            this.channel = null;
            this.log.debug(`${this.exchangeName} closing channel...`);
            try {
                channel.close();
                this.log.info(`${this.exchangeName} channel closed`);
            } catch (e) {
                this.log.error(`${this.exchangeName} unable to close channel: ` + err);
            }
        }
    }

    async _reconnect() {
        if (this.reconnecting) {
            return;
        }
        this.reconnecting = true;
        this._close();
        await sleep(500);
        try {
            await this._open();
            this.reconnecting = false;
        } catch (err) {
            this.log.error(`"${this.exchangeName}" unable to reconnect channel: ` + err);
            setTimeout(() => {
                this.reconnecting = false;
                this._reconnect();
            }, 3000);
        }
    }
}

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

        const ch = await this.rabbit.connection.createChannel();
        ch.assertExchange(this.exchangeName, 'topic', {durable: false});
        ch.on('error', onChannelError);
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

exports.Connection = RabbitConnection;
exports.Consumer = RabbitConsumer;
exports.Producer = RabbitProducer;

//
//  index.js ends here
