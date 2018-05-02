//  -*- coding: utf-8 -*-
//  index.js ---
//  created: 2017-03-09 21:24:25
//

'use strict';

const {EventEmitter} = require('events');
const assert = require('assert');
const amqp = require('amqplib/channel_api');

class RabbitConnection extends EventEmitter {

    /**
     * @param url {String} - the URL of the RabbitMQ server.
     * @param [options] {Object} - options.
     * @param [options.log=console] {Object} - logger.
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
            return connection.close()
                .then(
                    () => this.log.info('connection closed'),
                    err => this.log.error('unable to close connection: ' + err)
                )
                .then(() => this.emit('disconnected'));
        }
    }

    async deleteQueue(name) {
        const ch = await this.connection.createChannel();
        await ch.deleteQueue(name);
        await ch.close();
    }

    _onConnectionError(err) {
        this.log.error('amqp connection error: ' + err);
        this._reconnect();
    }

    _reconnect() {
        return this.disconnect()
        .then(() => this.connect())
        .catch(err => {
            this.log.error('unable to reconnect to amqp: ' + err);
            setTimeout(this._reconnect.bind(this), 3000);
        });
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
            this._open();
        });

        this.rabbit.on('disconnect', () => {
            this._close();
        });

        // already connected -> open queue
        if (this.rabbit.connection) {
            return this._open();
        }
    }

    async _msgReceived(msg) {
        try {
            const jsonContent = JSON.parse(msg.content.toString());
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

        let channel;

        // open channel
        return conn.createChannel()
        .then(ch => {
            channel = ch;
            ch.on('error', onChannelError);
            ch.on('close', onChannelClose);
            return ch.assertQueue(this.queueName , this.queueOptions);
        })
        .then(q => {
            this.routingKeys.forEach(key => {
                channel.bindQueue(q.queue, this.exchangeName, key);
            });

            channel.consume(q.queue, this._msgReceived.bind(this), this.consumeOptions);
            this.channel = channel;
            this.log.debug(`${this.exchangeName} waiting for events...`);
        });
    }

    async _close() {
        let promise = Promise.resolve(null);

        if (this.channel) {
            const channel = this.channel;
            this.channel = null;
            this.log.debug(`${this.exchangeName} closing channel...`);
            promise = promise.then(() => channel.close())
            .then(
                () => this.log.info(`${this.exchangeName} channel closed`),
                err => this.log.error(`${this.exchangeName} unable to close channel: ` + err));
        }

        return promise;
    }

    async _reconnect() {
        await this._close();
        try {
            await this._open();
        } catch (err) {
            this.log.error(`${this.exchangeName} unable to reconnect channel: ` + err);
            setTimeout(this._reconnect.bind(this), 3000);
        }
    }
}

exports.Connection = RabbitConnection;
exports.Consumer = RabbitConsumer;

//
//  index.js ends here
