/*
 * File name:    connection.js
 * File Created: 2018-10-07T23:03:42
 * Company:      Vinka
 */

'use strict';

const assert = require('assert');
const {EventEmitter} = require('events');
const amqp = require('amqplib/channel_api');
const {sleep} = require('./util');

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
        this.reconnecting = false;
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
                connection.close().catch(err => {
                    this.log.error('unable to close connection: ' + err.message);
                });
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
        if (this.reconnecting) {
            return;
        }
        this.reconnecting = true;
        await this.disconnect();
        try {
            await this.connect();
            this.reconnecting = false;
        } catch (err) {
            this.log.error('unable to reconnect to amqp: ' + err);
            setTimeout(() => {
                this.reconnecting = false;
                this._reconnect();
            }, 3000);
        }
    }
}

module.exports = RabbitConnection;
