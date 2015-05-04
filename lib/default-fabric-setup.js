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
    assert.object(opts.default_vlan, 'opts.default_vlan');
    assert.string(opts.default_vlan.name, 'default vlan name');
    assert.number(opts.default_vlan.vlan_id, 'default vlan id');
    assert.object(opts.default_network, 'network');
    assert.string(opts.default_network.gateway, 'default gateway');
    assert.string(opts.default_network.name, 'default name');
    assert.string(opts.default_network.provision_start_ip, 'start_ip');
    assert.string(opts.default_network.provision_end_ip, 'end_ip');
    assert.string(opts.default_network.resolvers, 'resolvers');
    assert.number(opts.default_network.vlan_id, 'vlan_id');

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'DefaultFabricSetupStream' }, true);
    self.dead = false;

    self.napiClient = new NAPI(opts.napi);

    function findDefaultVlan(_opts, cb) {
        assert.object(_opts, '_opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        self.napiClient.listFabricVLANs(_opts.uuid,
            reqOptions,
            function (err, vlans) {

            if (err) {
                return cb(err)
            }

            defaultVlan = vlans.reduce(function (acc, v) {
                return acc || (v.name === opts.default_vlan.name ? v : null);
            }, null);

            if (defaultVlan) {
                self.log.info({ vlan: defaultVlan, user: _opts.uuid },
                    'Default vlan exists for user');
                _opts.defaultVlan = defaultVlan;
                return cb();
            }

        });
    }

    function createDefaultVlan(_opts, cb) {
        assert.object(_opts, '_opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        if (_opts.defaultVlan) {
            return cb();
        }

        function create() {
            self.napiClient.createFabricVLAN(_opts.uuid,
                opts.default_vlan,
                reqOptions,
                function (_err, vlan) {
                var timeout = 10000;
                if (_err &&
                    _err.statusCode >= 500 && _err.statusCode < 600) {
                    self.log.error({ err: _err, user: _opts.uuid,
                        requestId: _opts.requestId },
                        'Server error creating vlan for user %s. ' +
                        'Retrying in %sms',
                        _opts.uuid, timeout);
                    setTimeout(create, timeout);
                } else if (_err) {
                    self.log.error({ err: _err },
                        'Error creating fabric vlan for user');
                    return cb(_err);
                }
                _opts.defaultVlan = vlan;
                _opts.createdVlan = true;
                self.log.info({ vlan: vlan, user: _opts.uuid },
                    'Created default fabric vlan for user');
                return cb();
            });
        }
        create();
    }

    function findDefaultNetwork(_opts, cb) {
        assert.object(_opts, 'opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        self.napiClient.listFabricNetworks(_opts.uuid,
            opts.default_network.vlan_id,
            reqOptions,
            function (err, networks) {
            var defaultNetwork;
            if (err) {
                self.log.error({ err: err, user: _opts.uuid,
                    requestId: _opts.requestId },
                    'Error creating default fabric network for user');
                return cb(err);
            }
            // XXX - this network is not necessarily the right one.
            defaultNetwork = networks.reduce(function (acc, n) {
                return n.name === opts.default_network.name ? n : null;
            }, null);
            if (defaultNetwork) {
                self.log.info({ network: defaultNetwork, user: _opts.uuid,
                    requestId: _opts.requestId },
                    'Default network exists for user',
                    defaultNetwork.uuid, _opts.uuid);
                _opts.defaultNetwork = defaultNetwork;
                return cb();
            }
        });
    }

    function createDefaultNetwork(_opts, cb) {
        assert.object(_opts, 'opts');
        assert.string(_opts.uuid, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        if (_opts.defaultNetwork) {
            return cb();
        }

        function create() {
            self.napiClient.createFabricNetwork(_opts.uuid,
                opts.default_network.vlan_id, opts.default_network,
                reqOptions,
                function (_err, network) {
                    var timeout = 10000;
                    if (_err &&
                        _err.statusCode >= 500 && _err.statusCode < 600) {
                        self.log.warn({ err: _err },
                            'Server error creating network for user %s. ' +
                            'Retrying in %sms',
                            _opts.uuid, timeout);
                        setTimeout(create, timeout);
                    } else if (_err) {
                        self.log.error({ err: _err, user: _opts.uuid },
                            'Error creating network for user %s',
                            _opts.uuid);
                        return cb(_err);
                    }
                    self.log.info({ network: network, user: _opts.uuid },
                        'create default network %s for user %s',
                        network.uuid, _opts.uuid);
                    _opts.defaultNetwork = network;
                    _opts.createdNetwork = true;
                    return cb();
                });
        }
        create();
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

        var msg;

        if (self.dead) {
            return cb();
        }

        self.log.debug({ user: obj.user }, 'Adding network for user');

        var arg = {
            uuid: obj.user.uuid,
            requestId: obj.requestId
        };

        vasync.pipeline({
            arg: arg,
            funcs: [
                findDefaultVlan,
                createDefaultVlan,
                findDefaultNetwork,
                createDefaultNetwork
            ]
        }, function (err, results) {
            if (err) {
                return cb(err);
            }
            obj.vlan = arg.defaultVlan;
            obj.network = arg.defaultNetwork;
            if (obj.createdNetwork || obj.createdVlan) {
                msg = 'created default overlay for user';
            } else {
                msg = 'found existing default overlay for user';
            }
            self.log.info({ obj: obj }, msg);
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

    if (self.dead) {
        return cb();
    }

    self.napiQueue.push(wrapTask(obj, cb), function (err) {
        // TODO: emit error here?

        if (err) {
            // does error/failure here depend on the type of error?
            // this is the appropriate level of encapsulation.
            self.emit('failure', err, obj);

            // most errors imply failure as well
            // some errors are 'success' at least that we don't want
            // to repeat - typically pass those through.
        }

        self.log.debug({ npending: self.napiQueue.npending,
        queued: self.napiQueue.queued.length },
        'finish: napiQueue has %d tasks running, %d queued',
        self.napiQueue.npending, self.napiQueue.queued.length);
    });
    self.log.debug({ npending: self.napiQueue.npending,
        queued: self.napiQueue.queued.length },
        'pushed: napiQueue has %d tasks running, %d queued',
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
