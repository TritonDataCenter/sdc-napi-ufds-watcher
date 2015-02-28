/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * UFDS user watcher
 */

var assert = require('assert-plus');
var clone = require('clone');
var mod_vasync = require('vasync');
var NAPI = require('sdc-clients').NAPI;
var UfdsClient = require('ufds');



// --- UFDSwatcher object



/**
 * UFDSwatcher constructor
 */
function UFDSwatcher(opts) {
    this.config = opts.config;
    this.log = opts.log;
    this.napi = opts.napi;
    this.ufds = opts.ufds;
}


/**
 * Start watching for users
 */
UFDSwatcher.prototype.watch = function _watch() {
    var self = this;

    this.baseDN = 'ou=users, o=smartdc';
    this.queue = new mod_vasync.queue(this.processUser.bind(self),
        this.config.concurrency);
    this.queue.drain = this.onQueueDrain.bind(this);

    // XXX: make this an ldapjs filter object?
    this.searchOpts = {
        filter: '(&' +
            '(objectclass=sdcperson)' +
            // filter out sub-users:
            '(!(account=*))' +
            '(!(defaultfabricsetup=true))' +
        ')',
        scope: 'sub'
    };

    this.poll();
};


/**
 * The queue is empty, so this batch of users has been processed.  Queue
 * another poll.
 */
UFDSwatcher.prototype.onQueueDrain = function _onQueueDrain(notDrained) {
    if (notDrained) {
        this.log.debug('setting poll timer');
    } else {
        this.log.debug('queue drained: setting poll timer');
    }

    // XXX: we might want to just go again immediately if we're in initial
    // "catchup" mode - namely, we just started
    setTimeout(this.poll.bind(this), this.config.pollInterval);
};


/**
 * Poll UFDS for users with the defaultfabricsetup property unset, and add
 * those users to the queue
 */
UFDSwatcher.prototype.poll = function _pollUFDS() {
    var self = this;
    this.log.debug({ baseDN: this.baseDN, searchOpts: this.searchOpts },
        'searching for users');

    this.ufds.search(this.baseDN, this.searchOpts,
            function _afterSearch(err, results) {
        if (err) {
            self.log.error({
                baseDN: self.baseDN,
                err: err,
                searchOpts: self.searchOpts
            }, 'Error searching UFDS');

            return self.onQueueDrain(true);
        }

        if (!results) {
            self.log.debug('No user results');
            return self.onQueueDrain(true);
        }

        results.forEach(function (user) {
            self.log.debug({ user: user }, 'Adding user to queue');
            self.queue.push(user);
        });
    });
};


/**
 * Process a user from the queue - add a default fabric network for them
 */
UFDSwatcher.prototype.processUser = function _processUser(user, callback) {
    this.log.debug({ user: user }, 'processing user');

    var defaults = clone(this.config.defaults);

    /*
     * defaults:
     *
     * - vlan params are in defaults.vlan
     * - network params are in defaults.network
     * - owner_uuid (of the user passed in) needs to be added to each of
     *   these
     */


    // XXX: if the user already has defaultfabricsetup set, don't create
    // another network


    /*
     * This is where the magic happens (or is supposed to).  In order, we
     * need to:
     *
     * - Create a default VLAN
     * - Create a default network on it
     */

    return callback();
};



// --- Exports



/**
 * Creates a new UFDSwatcher, and calls callback with it as an argument
 * once it has connected
 */
function createWatcher(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.general, 'opts.general');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.napi, 'opts.napi');
    assert.object(opts.networkParams, 'opts.networkParams');
    assert.object(opts.ufds, 'opts.ufds');
    assert.func(callback, 'callback');

    assert.number(opts.general.concurrency, 'opts.general.concurrency');
    assert.number(opts.general.pollInterval, 'opts.general.pollInterval');

    var napiConfig = clone(opts.napi);
    var ufdsConfig = clone(opts.ufds);

    napiConfig.log = opts.log.child({ component: 'napi' });
    ufdsConfig.log = opts.log.child({ component: 'ufds' });

    var ufds = new UfdsClient(ufdsConfig);
    var napi = new NAPI(napiConfig);

    ufds.once('connect', function () {
        return callback(null, new UFDSwatcher({
            config: opts.general,
            log: opts.log.child({ component: 'watcher' }),
            napi: napi,
            ufds: ufds
        }));
    });
}



module.exports = {
    create: createWatcher
};
