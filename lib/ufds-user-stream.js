/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream to filter out objects meeting some UFDS criteria.
 */

// expect:

var assert = require('assert-plus');
var Transform = require('stream').Transform;
var UFDS = require('ufds');
var util = require('util');
var vasync = require('vasync');

function UfdsUserStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufds, 'opts.ufds');

    Transform.call(this, { objectMode: true });
    var self = this;

    self.log = opts.log.child({ component: 'UfdsUserStream'}, true);

    // XXX - update opts here for retries, etc.
    self.ufdsClient = new UFDS(opts.ufds);
    // on first connect
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
    })

    // each obj is a changelog with a parsed 'changes' key.
    function ufdsWorker(obj, cb) {
        var err;
        if (!obj.uuid) {
            err = new Error(util.format, 'Changelog %d missing uuid', obj.changenumber);
        } else if (obj.uuid.length > 1) {
            var err = new Error(util.format, 'Changelog %d has extra uuid', obj.changenumber);
        }

        if (err) {
            self.log.error({ err: err, changelog: obj }, 'Can\'t determine UUID for changelog %d', obj.changenumber);
            return cb(err);
        }

        self.ufdsClient.getUser(obj.uuid[0], function(err, acct) {
            if (err) {
                self.log.error(err, 'UFDS error fetching user %s', obj.uuid[0]);
                return cb(err);
            }

            self.log.debug({acct: acct}, "Received account");

            // XXX what's the test, precisely?
            // objectclass=['sdcuser'] <= only, not ['sdcuser', 'sdcaccountuser']
            // lacks a leaf node for defaults
            if (acct.objectclass.length === 1 &&
                acct.objectclass[0] === 'sdcuser' &&
                acct.defaultFabricSetup === false) {
                self.push(acct);
            }
            return cb();
        });
    }

    this.ufdsQueue = vasync.queue(ufdsWorker, 10);
    // set up and manage client
    // set up and manage vasync queue
    // set up and manage filter function
    //
    // ufds client on connect should set this stream to "on".
}
util.inherits(UfdsUserStream, Transform);

// occasionally there are base64-encoded entries.
function parseChange(str, cb) {
    var _str = str;
    var json;
    var base64 = false;
    if (str.match(/^: /)) {
        _str = new Buffer(str, 'base64').toString('ut8');
        base64 = true;
    }
    try {
        json = JSON.parse(_str);
    } catch (e) {
        return cb(e);
    }
    return cb(null, json);
}

UfdsUserStream.prototype._transform = function(obj, _, cb) {
    // XXX if we're not connected?
    // think we'll add to the queue until internal mechanism of pipe
    // thinks it can't handle any more.
    parseChange(obj.changes, function (err, parsed) {
        if (err) {
            log.error({ err: e, changelog: obj }, "Unable to extract JSON from changenumber %d, skipping", obj.changenumber);
            return cb();
        }
        obj.changes = parsed;
        return queue.push(obj, cb);
    });
}

    // push the object to the queue; call cb() until queue is full.
    // depending on the result, queue will either push or unshift or error.

    // obj is a *changelog*
    // we check that is it a user, and filter the user.
}

module.exports = UfdsUserStream;
