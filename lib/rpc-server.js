/*
 * File name:    rpc-server.js
 * File Created: 2018-10-07T23:04:48
 * Company:      Vinka
 */

'use strict';

const {marshal, unmarshal} = require('./util');

class RpcServer {

    /**
     * @param rabbit {Rabbit} - rabbit connection.
     * @param handler {Function} - rpc server handler function.
     * @param [options] {Object} - options.
     * @param [options.logger=console] {Object} - logger.
     * @param [options.queue=''] {String} - name of the queue. (rabbit will auto-generate if not given)
     * @param [options.durable=false] {Boolean} - whether the queue should be durable or not.
     * @param [options.exclusive=false] {Boolean} - whether the queue should be exclusive or shared with consumers.
     * @param [options.autoDelete=false] {Boolean} - if true, queue will be deleted when all consumers disappear.
     * @param [options.messageTtl] - TTL for messages (in milliseconds).
     * @param [options.prefetchCount] - How many concurrent requests does the server allow (falsy=unlimited).
     */
    constructor(rabbit, handler, options={}) {
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
        this.prefetchCount = options.prefetchCount;
        this.handler = handler;
        this.channel = null;
    }

    async init() {
        this.rabbit.on('connected', () => this._open());
        this.rabbit.on('disconnected', () => this._close());

        // already connected -> just open channels
        if (this.rabbit.connection) {
            await this._open();
        }
    }

    async _msgReceived(msg) {
        let jsonContent;
        try {
            jsonContent = unmarshal(msg.content);
        } catch (e) {
            this.log.error('unable to unmarshal the message: ' + msg);
            // ack because we won't be able to unmarshal the message the next time anyway
            this._ack(msg);
            // send error response
            this.channel.sendToQueue(
                msg.properties.replyTo,
                marshal({_error: e.message}),
                {correlationId: msg.properties.correlationId});
            return;
        }
        this.log.debug(`rpc server got ${jsonContent}`);
        try {
            // execute rpc server handler
            const result = await this.handler(jsonContent);
            // success, ack and return response
            this._ack(msg);
            this.channel.sendToQueue(
                msg.properties.replyTo, 
                marshal(result),
                {correlationId: msg.properties.correlationId});
        } catch (e) {
            // rpc call failed
            this.log.error('unable to execute rpc call: ' + e);
            // ack and send err response
            this.channel.sendToQueue(
                msg.properties.replyTo,
                marshal({_error: e.message}),
                {correlationId: msg.properties.correlationId});
        }
    }

    async _open() {
        const ch = await this.rabbit.connection.createChannel();
        await ch.assertQueue(this.queueName, this.queueOptions);
        ch.prefetch(this.prefetchCount);
        ch.consume(this.queueName, this._msgReceived.bind(this), this.consumeOptions);
        this.channel = ch;
    }

    _ack(msg) {
        this.channel.ack(msg);
    }
}

module.exports = RpcServer;
