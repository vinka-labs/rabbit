/*
 * File name:    rpc-server.js
 * File Created: 2018-10-07T23:04:48
 * Company:      Vinka
 */

'use strict';

const {
    marshalRpcError,
    marshalRpcResult,
    unmarshal,
    addChannelMaintenance,
    openChannel
} = require('./util');

class RpcServer {

    /**
     * @param rabbit {Rabbit} - rabbit connection.
     * @param handlers {Object} - rpc server handler functions. Key is the name of the operation.
     *    Value is the handler function.
     * @param [options] {Object} - options.
     * @param [options.logger=console] {Object} - logger.
     * @param [options.queue=''] {String} - name of the queue. (rabbit will auto-generate if not given)
     * @param [options.durable=false] {Boolean} - whether the queue should be durable or not.
     * @param [options.exclusive=false] {Boolean} - whether the queue should be exclusive or shared with consumers.
     * @param [options.autoDelete=false] {Boolean} - if true, queue will be deleted when all consumers disappear.
     * @param [options.messageTtl] - TTL for messages (in milliseconds).
     * @param [options.prefetchCount] - How many concurrent requests does the server allow (falsy=unlimited).
     * @param [options.ackErrors] - Should the rpc server ack messages even if rpc handler function throws error.
     */
    constructor(rabbit, handlers, options={}) {
        if (!rabbit) {
            throw Error('missing rabbit instance');
        }

        if (!handlers) {
            throw Error('missing handlers');
        }

        this.log = options.logger || console;
        this.rabbit = rabbit;
        this.queueName = options.queue || '';
        this.queueOptions = {
            durable: !!options.durable,
            exclusive: !!options.exclusive,
            autoDelete: !!options.autoDelete,
        };
        this.consumeOptions = {
            noAck: false,
        };
        this._ackErrors = !!options.ackErrors;
        this.prefetchCount = options.prefetchCount;
        this._handlers = handlers;
        this.channel = null;

        addChannelMaintenance(this);
    }

    async _handler(operation, params) {
        const fun = this._handlers[operation];
        if (!fun) {
            throw Error(`unknown operation "${operation}"`);
        }
        return fun.call(this.handlers, ...params);
    }

    _ack(msg) {
        // we may not have channel anymore if the connection was closed
        // while executing the rpc handler
        if (this.channel) {
            this.channel.ack(msg);
        } else {
            throw Error('channel closed while executing the rpc handler');
        }
    }

    async _msgReceived(msg) {
        let rpccall;
        try {
            rpccall = unmarshal(msg.content);
        } catch (e) {
            this.log.error('unable to unmarshal the message: ' + msg);
            // ack because we won't be able to unmarshal the message the next time anyway
            this._ack(msg);
            // send error response
            this.channel.sendToQueue(
                msg.properties.replyTo,
                marshalRpcError(e),
                {correlationId: msg.properties.correlationId});
            return;
        }
        let {operation, params} = rpccall;
        this.log.debug(`rpc server got ${operation}(${params})`);
        try {
            // execute rpc server handler
            const result = await this._handler(operation, params);
            // success, ack and return response
            this._ack(msg);
            this.channel.sendToQueue(
                msg.properties.replyTo, 
                marshalRpcResult(result),
                {correlationId: msg.properties.correlationId});
        } catch (e) {
            // rpc call failed
            this.log.error('unable to execute rpc call: ' + e);
            // ack and send err response

            if (this._ackErrors) {
                this._ack(msg);
            }

            // we may not have channel anymore if the connection was closed
            // while executing the rpc handler
            if (this.channel) {
                this.channel.sendToQueue(
                    msg.properties.replyTo,
                    marshalRpcError(e),
                    {correlationId: msg.properties.correlationId});
            }
        }
    }

    async _open() {
        const ch = await openChannel(this);
        await ch.assertQueue(this.queueName, this.queueOptions);
        ch.prefetch(this.prefetchCount);
        ch.consume(this.queueName, this._msgReceived.bind(this), this.consumeOptions);
        this.channel = ch;
    }
}

module.exports = RpcServer;
