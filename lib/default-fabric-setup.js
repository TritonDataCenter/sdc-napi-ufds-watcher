/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
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
    assert.number(opts.default_network.mtu, 'mtu');

    Transform.call(this, { objectMode: true });

    var self = this;
    var log = opts.log.child({ component: 'DefaultFabricSetupStream' }, true);

    self.log = log;
    self.dead = false;

    self.napiClient = new NAPI(opts.napi);

    function findDefaultVlan(_opts, cb) {
        var userUUID = _opts.uuid;
        assert.object(_opts, '_opts');
        assert.string(userUUID, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        function find() {
            self.napiClient.listFabricVLANs(userUUID,
                reqOptions,
                function listCb(err, vlans) {
                    var timeout = 10000;

                    if (err && err.statusCode >= 500 && err.statusCode < 600) {
                        log.warn({err: err, user: userUUID},
                            'Server error finding vlan for user %s. ' +
                            'Retrying in %dms',
                            userUUID, timeout);
                        setTimeout(find, timeout);
                        return;
                    } else if (err) {
                        log.error({err: err, user: userUUID},
                            'Error finding vlan for user %s',
                            userUUID);
                        cb(err);
                        return;
                    }

                    var defaultVlanList;
                    defaultVlanList = vlans.filter(function matchNm(v) {
                        return (v.name === opts.default_vlan.name);
                    });

                    if (defaultVlanList.length === 0) {
                        log.info({ user: userUUID },
                            'Default vlan does not exist for user');
                    } else {
                        assert.ok(defaultVlanList === 1);
                        _opts.defaultVlan = defaultVlanList[0];
                        log.info({ vlan: _opts.defaultVlan,
                                   user: userUUID },
                            'Default vlan exists for user');
                    }

                    cb();
                    return;
                });
        }
        find();
    }

    function createDefaultVlan(_opts, cb) {
        var userUUID = _opts.uuid;
        assert.object(_opts, '_opts');
        assert.string(userUUID, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        if (_opts.defaultVlan) {
            return cb();
        }

        function create() {
            self.napiClient.createFabricVLAN(userUUID,
                opts.default_vlan,
                reqOptions,
                function (_err, vlan) {
                var timeout = 10000;
                if (_err &&
                    _err.statusCode >= 500 && _err.statusCode < 600) {
                    log.error({ err: _err, user: userUUID,
                        requestId: _opts.requestId },
                        'Server error creating vlan for user %s. ' +
                        'Retrying in %sms',
                        userUUID, timeout);
                    setTimeout(create, timeout);
                    return;
                } else if (_err) {
                    log.error({ err: _err },
                        'Error creating fabric vlan for user');
                    return cb(_err);
                }
                _opts.defaultVlan = vlan;
                _opts.createdVlan = true;
                log.info({ vlan: vlan, user: userUUID },
                    'Created default fabric vlan for user');
                return cb();
            });
        }
        create();
    }

    function findDefaultNetwork(_opts, cb) {
        var userUUID = _opts.uuid;
        assert.object(_opts, 'opts');
        assert.string(userUUID, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        function find() {
            self.napiClient.listFabricNetworks(userUUID,
                opts.default_network.vlan_id,
                {},
                reqOptions,
                function listCb(err, networks) {
                    var timeout = 10000;

                    if (err && err.statusCode >= 500 && err.statusCode < 600) {
                        log.warn({err: err, user: userUUID},
                            'Server error finding network for user %s. ' +
                            'Retrying in %dms',
                            userUUID, timeout);
                        setTimeout(find, timeout);
                        return;
                    } else if (err) {
                        log.error({err: err, user: userUUID},
                            'Error finding network for user %s',
                            userUUID);
                        cb(err);
                        return;
                    }
                    var defaultNetworkList;
                    defaultNetworkList = networks.filter(function matchNm(n) {
                        return (n.name === opts.default_network.name);
                    });

                    if (defaultNetworkList.length === 0) {
                        log.info({ user: userUUID },
                            'Default network does not exist for user');
                    } else {
                        assert.ok(defaultNetworkList === 1);
                        _opts.defaultNetwork = defaultNetworkList[0];
                        log.info({network: _opts.defaultNetwork,
                                  user: userUUID,
                                  requestId: _opts.requestId
                                 },
                                 'Default network exists for user',
                                 _opts.defaultNetwork.uuid,
                                 userUUID);
                    }

                    cb();
                    return;
            });
        }
        find();
    }

    function createDefaultNetwork(_opts, cb) {
        var userUUID = _opts.uuid;
        assert.object(_opts, 'opts');
        assert.string(userUUID, 'uuid');
        assert.string(_opts.requestId, 'requestId');

        var reqOptions = { headers: { 'x-request-id': _opts.requestId }};

        if (_opts.defaultNetwork) {
            return cb();
        }

        function create() {
            self.napiClient.createFabricNetwork(userUUID,
                opts.default_network.vlan_id, opts.default_network,
                reqOptions,
                function (_err, network) {
                    var timeout = 10000;
                    if (_err &&
                        _err.statusCode >= 500 && _err.statusCode < 600) {
                        log.warn({ err: _err, user: userUUID },
                            'Server error creating network for user %s. ' +
                            'Retrying in %sms',
                            userUUID, timeout);
                        setTimeout(create, timeout);
                        return;
                    } else if (_err) {
                        log.error({ err: _err, user: userUUID },
                            'Error creating network for user %s',
                            userUUID);
                        return cb(_err);
                    }
                    log.info({ network: network, user: userUUID },
                        'create default network %s for user %s',
                        network.uuid, userUUID);
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

        log.debug({ user: obj.user }, 'Adding network for user');

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
            log.info({ obj: obj }, msg);
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
