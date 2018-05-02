//  -*- coding: utf-8 -*-
//  hello-producer.js ---
//  created: 2018-04-18 01:02:12
//

var amqp = require('amqplib/callback_api');



amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
        const ex = 'jeditest';
        const key = process.env.KEY || 'anonymous.info';
        const msg = JSON.stringify({msg: `${process.env.MSG || 'hello'} ${Math.floor(Math.random() * 100)}`});

        ch.assertExchange(ex, 'topic', {
            durable: false
        });
        ch.publish(ex, key, new Buffer(msg));
        console.log(" [x] Sent %s:'%s'", key, msg);
    });

    setTimeout(function() {
        conn.close();
        process.exit(0)
    }, 500);
});

//
//  hello-producer.js ends here
