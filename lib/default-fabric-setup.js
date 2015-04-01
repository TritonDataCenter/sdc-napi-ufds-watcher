/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream to apply changes to napi/portolan based on
 * incoming UFDS objects.
 */


var assert = require('assert-plus');
var NAPI = require('sdc-clients').NAPI;
var Transform = require('stream').Transform;
var util = require('util');
var vasync = require('vasync');
var vstream = require('vstream');

function DefaultFabricSetupStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.napi, 'opts.napi');
    assert.object(opts.defaults, 'opts.defaults');

    Transform.call(this, { objectMode: true });

    // vstream.wrapTransform(this);

    var self = this;

    self.log = opts.log.child({ component: 'DefaultFabricSetupStream' }, true);

    self.napiClient = new NAPI(opts.napi);

    // expects objs like:
    // {
    //     user: { UFDS user },
    //     changenumber: 12
    // }
    function napiWorker(obj, cb) {

        // TODO: create overlay vlan, create overlay network.

        var params = {
            provisionable_by: obj.user.uuid
        };
        self.log.debug({ user: obj.user }, 'Adding network for user %s', obj.user.uuid);
        self.napiClient.listNetworks(params, function (err, networks) {
            if (err) {
                // XXX what err!!?
                self.log.error(err, 'NAPI err');
                return cb(err);
            }
            self.log.debug({ user: obj.user,
                network: 'network-not-actually-made-yet' },
                'Added network for user %s', obj.user.uuid);
            obj.network = { uuid: 'network-not-actually-made-yet' };
            self.push(obj);
            return cb();
        });
    }

    this.napiQueue = vasync.queue(wrapWorker(napiWorker), 10);
}
util.inherits(DefaultFabricSetupStream, Transform);
module.exports = DefaultFabricSetupStream;

// wraps a stream object such that it calls the stream cb when the task
// is started by vasync.queue
function wrapTask(obj, streamCb) {
    return function task() {
        streamCb();
        return obj;
    };
}

// wraps a typical vasync.queue worker to call a stream callback
function wrapWorker(workerFunc) {
    return function worker(wrappedObj, cb) {
        workerFunc(wrappedObj(), cb);
    };
}

DefaultFabricSetupStream.prototype._transform =
function _transform(obj, _, cb) {
    var self = this;
    self.napiQueue.push(wrapTask(obj, cb), function (err) {
        // TODO: emit error here?

        if (err) {
            // does error/failure here depend on the type of error?
            // this is the appropriate level of encapsulation.
            self.emit('failure', err, obj);
        }

        self.log.debug({ npending: self.napiQueue.npending,
        queued: self.napiQueue.queued.length },
        'finish: napiQueue has %d tasks running, %d queued',
        self.napiQueue.npending, self.napiQueue.queued.length)
    });
    self.log.debug({ npending: self.napiQueue.npending,
        queued: self.napiQueue.queued.length },
        'push: napiQueue has %d tasks running, %d queued',
        self.napiQueue.npending, self.napiQueue.queued.length);
};
