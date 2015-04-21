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
var uuid = require('node-uuid');
var vasync = require('vasync');

function DefaultFabricSetupStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.napi, 'opts.napi');
    assert.object(opts.defaults, 'opts.defaults');
    assert.object(opts.defaults.vlan, 'vlan');
    assert.string(opts.defaults.vlan.name, 'default vlan name');
    assert.number(opts.defaults.vlan.vlan_id, 'default vlan id');
    assert.object(opts.defaults.network, 'network');
    assert.string(opts.defaults.network.gateway, 'default gateway');
    assert.string(opts.defaults.network.name, 'default name');
    assert.string(opts.defaults.network.provision_start_ip, 'provision_start_id');
    assert.string(opts.defaults.network.provision_end_ip, 'provision_end_ip');
    assert.arrayOfString(opts.defaults.network.resolvers, 'resolvers');
    assert.number(opts.defaults.network.vlan_id, 'vlan_id');

    Transform.call(this, { objectMode: true });

    // vstream.wrapTransform(this);

    var self = this;

    self.log = opts.log.child({ component: 'DefaultFabricSetupStream' }, true);
    self.dead = false;

    self.napiClient = new NAPI(opts.napi);

    function createDefaultVlan(_opts, cb) {
        assert.object(_opts, '_opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        self.napiClient.listFabricVLANs(_opts.uuid,
            { headers: { 'x-request-id': _opts.requestId }},
            function (err, vlans) {
            var defaultVlan;
            if (err) {
                return cb(err);
            }
            defaultVlan = vlans.reduce(function (acc, v) {
                return v.name === opts.defaults.vlan.name ? v : null;
            }, null);
            if (defaultVlan) {
                self.log.info({ vlan: defaultVlan, user: _opts.uuid },
                    'Default vlan %s exists for user %s', defaultVlan.uuid,
                    _opts.uuid);
                return cb();
            }

            // XXX CREATE
            self.napiClient.createFabricVLAN(_opts.uuid,
                opts.defaults.vlan,
                { headers: { 'x-request-id': _opts.requestId }},
                function (_err, vlan) {
                if (_err) {
                    self.log.error({ err: _err },
                        'Error creating vlan for user %s', _opts.uuid);
                    return cb(_err);
                }
                self.log.info({ vlan: vlan, user: _opts.uuid },
                    'Created default vlan %s for user %s', vlan, _opts.uuid);
                return cb();
            });
        });
    }

    function createDefaultNetwork(_opts, cb) {
        assert.object(opts, 'opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        self.napiClient.listFabricNetworks(_opts.uuid, opts.defaults.network.vlan_id,
            { headers: { 'x-request-id': _opts.requestId }},
            function (err, networks) {
            var defaultNetwork;
            if (err) {
                self.log.error({ err: err, user: _opts.uuid },
                    'Error creating network for user %s', _opts.uuid);
                return cb(err);
            }
            defaultNetwork = networks.reduce(function(acc, n) {
                return n.name === opts.defaults.network.name ? n : null;
            }, null);
            if (defaultNetwork) {
                self.log.info({ network: defaultNetwork, user: _opts.uuid },
                    'Default network %s exists for user %s',
                    defaultNetwork.uuid, _opts.uuid);
                return cb();
            }

            self.napiClient.createFabricNetwork(_opts.uuid,
                opts.defaults.network.vlan_id, opts.defaults.network,
                function (_err, network) {
                    if (_err) {
                        self.log.error({ err: _err, user: _opts.uuid },
                            'Error creating network for user %s', _opts.uuid);
                        return cb(_err);
                    }
                    self.log.info({ network: network, user: _opts.uuid },
                        'create default network %s for user %s', network.uuid,
                        _opts.uuid);
                    return cb();
            });
        });
    }

    // expects objs like:
    // {
    //     user: { UFDS user },
    //     changenumber: 12,
    //     requestId: UUID
    // }
    function napiWorker(obj, cb) {
        assert.string(obj.user.uuid, 'user uuid');
        assert.string(obj.requestId, 'requestId');

        // TODO: create overlay vlan, create overlay network.

        if (self.dead) {
            return cb();
        }

        // var params = {
        //     provisionable_by: obj.user.uuid
        // };
        self.log.debug({ user: obj.user }, 'Adding network for user %s', obj.user.uuid);

        var arg = {
            uuid: obj.user.uuid,
            requestId: obj.requestId
        };

        vasync.pipeline({
            arg: arg,
            funcs: [
                createDefaultVlan,
                createDefaultNetwork
            ]
        }, function (err, results) {
            if (err) {
                return cb(err);
            }
            self.log.info({ user: obj.user.uuid },
                'created default overlay for user %s', obj.user.uuid);
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

    if (self.dead) {
        return cb();
    }

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
        self.napiQueue.npending, self.napiQueue.queued.length);
    });
    self.log.debug({ npending: self.napiQueue.npending,
        queued: self.napiQueue.queued.length },
        'push: napiQueue has %d tasks running, %d queued',
        self.napiQueue.npending, self.napiQueue.queued.length);
};


DefaultFabricSetupStream.prototype.close = function close() {
    var self = this;
    self.log.debug('close: entered');
    if (self.dead) {
        setImmediate(self.emit.bind(self, 'close'));
        return;
    }

    self.dead = true;
    self.push(null);
    self.emit('close');

    self.log.debug('close: done');
};
