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

    async rpc(operation, ...params) {
        return new Promise((res, rej) => {
            const corr = uuid();
            this.channel.consume(this.queue.queue, async msg => {
                // handle rpc response

                if (msg.properties.correlationId !== corr) {
                    // wrong correlation id, it must be for some other pending promise
                    this.channel.nack(msg);
                    return;
                }

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
        })
    }
}

module.exports = RpcClient;
