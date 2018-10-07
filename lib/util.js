/*
 * File name:    util.js
 * File Created: 2018-10-07T23:06:55
 * Company:      Vinka
 */

'use strict';

exports.sleep = ms => new Promise(r => setTimeout(r, ms));
exports.marshal = obj => Buffer.from(JSON.stringify(obj));
exports.unmarshal = data => JSON.parse(data.toString());
