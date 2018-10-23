/*
 * File name:    consumer.js
 * File Created: 2018-10-07T23:04:69
 * Company:      Vinka
 */

'use strict';

const assert = require('assert');
const {EventEmitter} = require('events');
const {unmarshal, addChannelMaintenance, openChannel} = require('./util');

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

        addChannelMaintenance(this);
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
        const channel = await openChannel(this);

        // open channel
        const q = await channel.assertQueue(this.queueName , this.queueOptions);
        this.routingKeys.forEach(key => {
            channel.bindQueue(q.queue, this.exchangeName, key);
        });

        channel.consume(q.queue, this._msgReceived.bind(this), this.consumeOptions);
        this.channel = channel;
        this.log.debug(`${this.exchangeName} waiting for events...`);
    }
}

module.exports = RabbitConsumer;
