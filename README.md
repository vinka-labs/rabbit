# Simplified RabbitMQ client

Installation

    npm install @vinka/rabbit

Example usage:

```javascript
const {Consumer} = require('@vinka/rabbit');

const ES_EXCHANGE_NAME = 'es_events';
const QUEUE = 'my-consumer';

const init = async function () {
    rabbit = new Consumer(require('../infra/rabbit'), ES_EXCHANGE_NAME, {
        logger: log,
        queue: QUEUE,
        ack: true,
        keys: [
            'viper.perform',
            'viper.pings',
            'viper.activateVehicle',
            'viper.deactivateVehicle',
        ],
    });
    internals.rabbit.on('msg', receiver);
    await internals.rabbit.listen();
    internals.etaSweeper.start();
};

```