/*
 * File name:    rpc-client.js
 * File Created: 2018-10-07T23:04:87
 * Company:      Vinka
 */

'use strict';

const uuid = require('uuid/v4');
const {marshal, unmarshal} = require('./util');

class RpcClient {
    constructor(rabbit, queueName, options = {}) {
        this.log = options.logger || console;
        this.serverQueueName = queueName;
        this.rabbit = rabbit;
        this.channel = null;
        this.myQueue = null;
    }

    async init() {
        this.rabbit.on('connected', () => this._open());
        this.rabbit.on('disconnected', () => this._close());

        // already connected -> just open channels
        if (this.rabbit.connection) {
            await this._open();
        }
    }

    async _open() {
        const onChannelClose = msg => {
            // If rabbit server goes down,
            // the only way we notice the outage is that channel is closed
            this.rabbit.reconnect();
            this.log.info(`Queue ${this.queueName} channel closed: ` + msg);
        };
        const ch = await this.rabbit.connection.createChannel();
        ch.on('close', onChannelClose);
        const q = await ch.assertQueue('', {exclusive: true});
        this.channel = ch;
        this.queue = q;
    }

    async _close() {
        if (this.channel) {
            const channel = this.channel;
            this.channel = null;
            this.log.debug(`Queue ${this.queueName} closing channel...`);
            try {
                await channel.close();
                this.log.info(`Queue ${this.queueName} channel closed`);
            } catch (e) {
                this.log.error(`Queue ${this.queueName} unable to close channel: ` + e.message);
            }
        }
    }

    async rpc(msg) {
        return new Promise((res, rej) => {
            const corr = uuid();
            this.channel.consume(this.queue.queue, async msg => {
                // handle rpc response

                if (msg.properties.correlationId !== corr) {
                    // wrong correlation id, it must be for some other pending promise
                    this.log.info('wrong corr id');
                    this.channel.nack(msg);
                    return;
                }

                let result;
                try {
                    result = unmarshal(msg.content);
                } catch (e) {
                    // error parsing the message
                    this.log.error(`unable to parse the rpc response: ` + e.message);
                    this.channel.ack(msg);
                    return;
                }
                if (result._error) {
                    rej(Error(result._error));
                    this.channel.ack(msg);
                    return;
                }
                try {
                    await res(result);
                    // successfully handled -> ack
                    this.channel.ack(msg);
                } catch (e) {
                    // handler error -> nack
                    this.log.error(`error handling rpc response: ` + e.message);
                    setTimeout(() => this.channel.nack(msg), 5000);
                }
            }, {noAck: false});
            this.channel.sendToQueue(
                this.serverQueueName,
                marshal(msg),
                {correlationId: corr, replyTo: this.queue.queue}
            );
        })
    }
}

module.exports = RpcClient;
