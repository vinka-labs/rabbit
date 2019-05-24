/*
 * File name:    rpc-client.js
 * File Created: 2018-10-07T23:04:87
 * Company:      Vinka
 */

'use strict';

const uuid = require('uuid/v4');
const {marshalRpcCall, unmarshal, addChannelMaintenance, openChannel} = require('./util');

class RpcClient {
    constructor(rabbit, queueName, options = {}) {
        this.log = options.logger || console;
        this.serverQueueName = queueName;
        this.rabbit = rabbit;
        this.channel = null;
        this.myQueue = null;

        addChannelMaintenance(this);
    }

    async _open() {
        const ch = await openChannel(this);
        const q = await ch.assertQueue('', {exclusive: true});
        this.channel = ch;
        this.queue = q;
    }

    /**
     * Execute rpc call.
     *
     * @param {string} operation - name of the operation.
     * @param {any[]} params - all parameters.
     * @param {Object} [options] - options.
     * @param {number} [options.timeout] - operation timeout in ms. Waits
     *     response forever if not given.
     */
    async exec(operation, params=[], options={}) {
        return this._rpc(operation, params, options);
    }

    /**
     * Execute rpc call.
     *
     * Same as exec() but doesn't take options (timeout) and operation
     * parameters are given as normal arguments instead of in one array.
     *
     * @param {string} operation - name of the operation.
     * @param {any[]} params - all parameters.
     * @param {Object} [options] - options.
     * @param {number} [options.timeout] - operation timeout in ms. Waits
     *     response forever if not given.
     */
    async rpc(operation, ...params) {
        return this._rpc(operation, params)
    }

    async _rpc(operation, params, options={}) {
        return new Promise((res, rej) => {
            const corr = uuid();
            let timeoutTimer;

            function clearTimeout() {
                if (timeoutTimer !== null) {
                    try {
                        clearTimeout(timeoutTimer);
                    } catch (e) {}
                }
            }

            this.channel.consume(this.queue.queue, async msg => {
                // handle rpc response

                if (msg.properties.correlationId !== corr) {
                    // wrong correlation id, it must be for some other pending promise
                    this.channel.nack(msg);
                    return;
                }

                clearTimeout();

                let result;
                try {
                    result = unmarshal(msg.content);
                } catch (e) {
                    // error parsing the message
                    this.log.error(`unable to parse the rpc response: ` + e.message);
                    rej(e);
                    this.channel.ack(msg);
                    return;
                }
                if (result.error) {
                    rej(Error(result.error));
                    this.channel.ack(msg);
                    return;
                }
                try {
                    await res(result.result);
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
                marshalRpcCall(operation, ...params),
                {correlationId: corr, replyTo: this.queue.queue}
            );
            if (options.timeout) {
                timeoutTimer = setTimeout(() => {
                    rej(Error(`timeout ${timeout / 1000}s. for ${msg.operation}`));
                }, options.timeout);
            }
        })
    }
}

module.exports = RpcClient;
