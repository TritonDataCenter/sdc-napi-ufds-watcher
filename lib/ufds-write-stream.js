/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream to apply changes to UFDS.
 */

// expect:

var assert = require('assert-plus');
var backoff = require('backoff');
var Transform = require('stream').Transform;
var UFDS = require('ufds');
var util = require('util');
var vasync = require('vasync');

// Really is the same as UfdsUserStream except instead of fetching,
// it's updating. Which amounts to the task to apply to the queue.
// XXX - factor those out.
function UfdsFabricSetupStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufds, 'opts.ufds');

    Transform.call(this, { objectMode: true });
    var self = this;

    self.log = opts.log.child({ component: 'UfdsFabricSetupStream' }, true);

    // XXX - what are good opts for reconnect/retry? ping p$
    self.ufdsClient = new UFDS(opts.ufds);

        ufdsClient.once('connect', function() {
        self.log.info('UFDS: connected');

        ufdsClient.removeAllListeners('error');
        ufdsClient.on('error', function (err) {
            self.log.error(err, 'UFDS: unexpected error');
            // XXX how to handle? emit error?
        });

        ufdsClient.on('close', function() {
            self.log.warn('UFDS: disconnected');
        });

        ufdsClient.on('connect', function() {
            self.log.info('UFDS: reconnected');
        });
    });

    ufdsClient.once('error', function() {
        self.log.error(err, 'UFDS unable to connect');
        // XXX how to handle? emit error?
    });

    function ufdsWorker(obj, cb) {
        var changes = {
            defaultFabricSetup: "true",
            defaultFabric: JSON.stringify(obj.network)
        }
        ufdsClient.updateUser(obj.user.uuid, changes, function (err, user) {
            if (err) {
                self.log.fatal({ err: err, user: user, network: network }, 'UFDS err writing default fabric information');
                return cb(err);
            }
            self.push(obj);
            return cb();
        });
    }

    var ufdsQueue = vasync.queue(ufdsWorker, 10);
}
util.inherits(UfdsFabricSetupStream, Transform);

UfdsFabricSetupStream.prototype._transform = function (obj, _, cb) {
    // XXX - if we're not connected?
    // obj is actually user, network object.
    // maybe unnecessary; network will have owner?
    queue.push(obj, cb);
}


