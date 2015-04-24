/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream from changelog entries to user objects
 */

// expect:

var assert = require('assert-plus');
var Transform = require('stream').Transform;
var util = require('util');
var mod_uuid = require('node-uuid');
var mod_vasync = require('vasync');

function UfdsUserStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufds, 'opts.ufds');

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'UfdsUserStream'}, true);
    self.ufdsClient = opts.ufds;
    self.dead = false;

    // each obj is a changelog with a parsed 'changes' property.
    function ufdsWorker(obj, cb) {
        var err;
        var uuid;
        var account;

        if (self.dead) {
            return cb();
        }

        if (!(obj && obj.changes && obj.changes.uuid) ||
            !obj.changes.uuid.length) {
            err = new Error(util.format,
                'Changelog %d missing uuid', obj.changenumber);
            self.log.error({ err: err, changelog: obj },
                'Can\'t determine UUID for changelog %d', obj.changenumber);
            return cb(err);
        } else if (obj.changes.uuid.length > 1) {
            err = new Error(util.format,
                'Changelog %d has extra uuid', obj.changenumber);
            self.log.error({ err: err, changelog: obj },
                'Can\'t determine UUID for changelog %d', obj.changenumber);
            return cb(err);
        } else {
            uuid = obj.changes.uuid[0];
        }

        if ((obj && obj.changes && obj.changes.account) &&
            obj.changes.account.length !== 1) {
            account = obj.changes.account[0];
        }

        self.ufdsClient.getUser(uuid, account, function (ufdsErr, user) {
            if (ufdsErr && ufdsErr.name === 'NoSuchObjectError') {
                return cb();
            } else if (ufdsErr && ufdsErr.statusCode >= 500 &&
                ufdsErr.statusCode < 600) {
                var timeout = 10000;
                self.log.warn(ufdsErr, 'UFDS server error fetching user %s.' +
                    ' Retrying in %sms', uuid, timeout);
                setTimeout(function () {
                    self.ufdsQueue.push(wrapTask(obj, cb));
                    self.log.debug({ npending: self.ufdsQueue.npending,
                        queued: self.ufdsQueue.queued.length },
                        'push: ufdsQueue has %d tasks running, %d queued',
                        self.ufdsQueue.npending, self.ufdsQueue.queued.length);
                }, timeout);
                return cb();
            } else if (ufdsErr) {
                self.log.error(ufdsErr, 'UFDS error fetching user %s', uuid);
                return cb(ufdsErr);
            }

            var writeObj = {
                user: user,
                changenumber: obj.changenumber,
                requestId: obj.requestId || mod_uuid.v4()
            };
            self.log.debug({ obj: writeObj },
                'Found user %s at changenumber %d',
                writeObj.user.uuid, writeObj.changenumber);
            self.push(writeObj);
            return cb();
        });
    }

    this.ufdsQueue = mod_vasync.queue(wrapWorker(ufdsWorker), 10);
}
util.inherits(UfdsUserStream, Transform);
module.exports = UfdsUserStream;


function parseChange(str, cb) {
    var _str = str;
    var json;
    // XXX - sometimes base64 returned by openldap, unsure if we get the same
    // via ldapjs, but just in case:
    // var base64 = false;
    // if (str.match(/^: /)) {
    //     _str = new Buffer(str, 'base64').toString('ut8');
    //     base64 = true;
    // }
    try {
        json = JSON.parse(_str);
    } catch (e) {
        return cb(e);
    }
    return cb(null, json);
}

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

UfdsUserStream.prototype._transform = function _transform(obj, _, cb) {
    var self = this;

    if (self.dead) {
        return cb();
    }

    parseChange(obj.changes, function (err, parsed) {
        if (err) {
            // XXX - Need to shout louder about these?
            self.log.error({ err: err, changelog: obj },
                'Unable to extract JSON from changenumber %d, skipping',
                obj.changenumber);
            return cb();
        }
        obj.changes = parsed;

        self.ufdsQueue.push(wrapTask(obj, cb), function (_err, data) {
            // XXX - this is the callback called when the task cb(err)'s
            // should we emit at this point (since we already called
            // _transform's cb()?)
            self.log.debug({ npending: self.ufdsQueue.npending,
            queued: self.ufdsQueue.queued.length },
            'finish: ufdsQueue has %d tasks running, %d queued',
            self.ufdsQueue.npending, self.ufdsQueue.queued.length);
        });
        self.log.debug({ npending: self.ufdsQueue.npending,
            queued: self.ufdsQueue.queued.length },
            'push: ufdsQueue has %d tasks running, %d queued',
            self.ufdsQueue.npending, self.ufdsQueue.queued.length);
        return;
    });
};


UfdsUserStream.prototype.close = function close() {
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
