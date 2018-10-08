# Simplified RabbitMQ client

Installation

    npm install @vinka/rabbit

Example event listener usage:

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

Example RPC server:

```javascript

const { Connection, RpcServer } = require('@vinka/rabbit');

const options = {
    queue: 'my-rpc-queue',
    prefetchCount: 3,       // allow three concurrent requests
};

const rpcFunc = async (msg) => {
    if (msg === 'rougned') {
        // if response is an object with property `_error`, this is automatically
        // translated to error and raises an error on the rpc client
        return {_error: 'not greeting odor'};
    }

    return `hello ${msg}`;
}

(async function connect() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    const consumer = new RpcServer(rabbit, rpcFunc, options);
    await consumer.init()
})();

```

Example RPC client:

```javascript

const { Connection, RpcClient } = require('@vinka/rabbit');

async function connect() {
    const rabbit = new Connection('amqp://localhost');
    await rabbit.connect();
    const client = new RpcClient(rabbit, 'my-rpc-queue', {logger: console});
    await client.init();
    return client;
}

async function exec() {
    const client = await connect();
    const reply = client.rpc('tulowitzki');
    console.log(reply); // "hello tulowitzki"
}

```
