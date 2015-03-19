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


var assert = require('assert-plus');
var backoff = require('backoff');
var Transform = require('stream').Transform;
var UFDS = require('ufds');
var util = require('util');
var vasync = require('vasync');

function UfdsWriteStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufds, 'opts.ufds');
    assert.string(opts.datacenter_name);

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'UfdsWriteStream' }, true);

    var ufdsClient = new UFDS(opts.ufds);

    ufdsClient.once('connect', function () {
        self.log.info('UFDS: connected');

        ufdsClient.removeAllListeners('error');
        ufdsClient.on('error', function (err) {
            self.log.error(err, 'UFDS: unexpected error');
        });

        ufdsClient.on('close', function () {
            self.log.warn('UFDS: disconnected');
        });

        ufdsClient.on('connect', function () {
            self.log.info('UFDS: reconnected');
        });
    });

    ufdsClient.once('error', function (err) {
        self.log.error(err, 'UFDS unable to connect');
    });

    function ufdsWorker(obj, cb) {
        var dclocalconfig = {
            dclocalconfig: opts.datacenter_name,
            defaultFabricSetup: 'true',
            defaultNetwork: obj.network.uuid
        };

        ufdsClient.updateDcLocalConfig(obj.user.uuid, obj.user.account,
            opts.datacenter_name, dclocalconfig, function (err, dclc) {
            if (err) {
                self.log.error({ err: err, obj: obj },
                    'Error writing dclocalconfig to UFDS for %s, network %s',
                    obj.user.uuid, obj.network);
                // XXX fail this changelog.
            }
            // XXX pass this changelog.
            obj.user.dclocalconfig = dclc;
            self.push(obj); // always push?
            return cb(err);
        });
    }

    this.ufdsQueue = vasync.queue(wrapWorker(ufdsWorker), 10);
}
util.inherits(UfdsWriteStream, Transform);
module.exports = UfdsWriteStream;

// XXX - generalize the wrap*, _transform, queue?
// wraps a stream object such that it calls the stream cb when the task
// is started by vasync.queue
function wrapTask(obj, streamCb) {
    return function () {
        streamCb();
        return obj;
    };
}

// wraps a typical vasync.queue worker to call a stream callback
function wrapWorker(workerFunc) {
    return function (wrappedObj, cb) {
        workerFunc(wrappedObj(), cb);
    };
}

UfdsWriteStream.prototype._transform = function (obj, _, cb) {
    var self = this;
    self.ufdsQueue.push(wrapTask(obj, cb), function () {
        self.log.debug({ npending: self.ufdsQueue.npending,
        queued: self.ufdsQueue.queued.length },
        'finish: ufdsWriteQueue has %d tasks running, %d queued',
        self.ufdsQueue.npending, self.ufdsQueue.queued.length);
    });
    self.log.debug({ npending: self.ufdsQueue.npending,
        queued: self.ufdsQueue.queued.length },
        'push: ufdsWriteQueue has %d tasks running, %d queued',
        self.ufdsQueue.npending, self.ufdsQueue.queued.length);
};
