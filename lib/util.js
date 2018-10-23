/*
 * File name:    util.js
 * File Created: 2018-10-07T23:06:55
 * Company:      Vinka
 */

'use strict';

exports.sleep = ms => new Promise(r => setTimeout(r, ms));
exports.marshal = obj => Buffer.from(JSON.stringify(obj));
exports.unmarshal = data => JSON.parse(data.toString());
exports.marshalRpcCall = (operation, ...params) => exports.marshal({operation, params});
exports.marshalRpcResult = result => exports.marshal({result});
exports.marshalRpcError = error => {
    if (error instanceof Error) {
        return exports.marshal({error: error.message});
    } else {
        return exports.marshal({error});
    }
};

/**
 * Mixin functions for channel wrappers (rpc client, rpc server, event producer, event consumer).
 */
const channelMixin = {

    /**
     *
     */
    async init() {
        this.rabbit.on('connected', () => this._reconnect());
        this.rabbit.on('disconnected', () => this._close());

        // already connected -> open queue
        if (this.rabbit.connection) {
            return this._reconnect();
        }
    },

    /**
     * Open and return new channel.
     *
     * Caller still needs to configure/consumer the channel. This function merely
     * creates the channel and assigns error listeners so that the connection
     * can be re-established after connection errors.
     */
    async _open() {
        const conn = this.rabbit.connection;

        if (!conn) {
            throw Error(`no rabbit connection`);
        }

        const onChannelError = err => {
            this.log.error(`channel error: ` + err);
            setTimeout(this._reconnect.bind(this), 3000);
        };

        // if rabbit server goes down,
        // the only way we notice the outage is that channel is closed
        const onChannelClose = msg => {
            this.log.info(`channel closed abrubtly`);
            this.rabbit.reconnect();
        };

        const channel = await conn.createChannel();
        channel.on('error', onChannelError);
        channel.on('close', onChannelClose);

        return channel;
    },

    /**
     * Close the channel.
     */
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
    },

    /**
     * Reconnect the channel.
     */
    async _reconnect() {
        if (this.reconnecting) {
            return;
        }
        this.reconnecting = true;
        this._close();
        // just to prevent busy loop if exchange doesn't exist for example
        await exports.sleep(500);
        try {
            await this._open();
            this.reconnecting = false;
        } catch (err) {
            this.log.error(`unable to reconnect channel: ` + err);
            setTimeout(() => {
                this.reconnecting = false;
                this._reconnect();
            }, 3000);
        }
    }
};

/**
 * Shortcut for calling the original mixin _open() function from your own
 * overriden _open() implementation.
 */
exports.openChannel = async self => {
    return channelMixin._open.call(self);
};

/**
 * Add connection/channel maintenance mixin functions to your channel wrapper.
 */
exports.addChannelMaintenance = self => {
    Object.keys(channelMixin).forEach(func => {
        if (!self[func]) {
            self[func] = channelMixin[func].bind(self);
        }
    });
};