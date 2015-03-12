/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream to apply changes to napi/portolan based on incoming UFDS objects.
 */


var assert = require('assert-plus');
var NAPI = require('sdc-clients').NAPI;
var Transform = require('stream').Transform;
var util = require('util');
var vasync = require('vasync');

function NapiFabricSetupStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.napi, 'opts.napi');
    assert.object(opts.defaults, 'opts.defaults');

    Transform.call(this, { objectMode: true });
    var self = this;

    self.log = opts.log.child({ component: 'NapiFabricSetupStream' }, true);

    self.napiClient = new NAPI(opts.napi);

    // each obj is a UFDS user.
    function napiWorker(obj, cb) {
        // XXX - convert to what actually works.
        // XXX - per Rob, at the very least we'll have to append to 'name'
        // to avoid a uniqueness constraint.
        self.napiClient.createNetwork(opts.defaults, function(err, network) {
            if (err) {
                log.fatal(err, 'NAPI network creation error');
                return cb(err);
            }

            var result = {
                user: obj,
                network: network
            }
            self.log.debug({ user: obj.uuid, network: network} , 'Created default overlay');
            self.push(result);
            return cb();
        });
    }

    this.napiQueue = vasync.queue(napiWorker, 10);
}
util.inherits(NapiFabricSetupStream, Transform);

NapiFabricSetupStream.prototype._transform = function _transform(obj, _, cb) {
    queue.push(obj, cb);
}

module.exports = NapiFabricSetupStream;
