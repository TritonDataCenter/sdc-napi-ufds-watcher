/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * A stream to manage changelog numbers and changelog processing
 */

var assert = require('assert-plus');
var Checkpoint = require('./checkpoint');
var Transform = require('stream').Transform;
var util = require('util');
var vasync = require('vasync');

function ChangenumberStartStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'ChangenumberStartStream' }, true);

    self.inflight = {};
    self.completed = [];

    self.checkpoint = opts.checkpoint;
}
util.inherits(ChangenumberStartStream, Transform);
module.exports = ChangenumberStartStream;

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

ChangenumberStartStream.prototype.updateCheckpoint =
function updateCheckpoint(cb) {

    // safe number to persist is highest completed that is less than
    // the lowest inflight.
    var self = this;

    var lowestInFlight = Object.keys(self.inflight).sort(
        function (a, b) {
        return a > b;
    })[0] || Infinity;
    var candidate = self.completed.reduce(function (acc, e) {
        if (e > acc && e <= lowestInFlight) {
            acc = e;
        }
        return acc;
    }, 0);
    self.log.debug({ inflight: self.inflight, completed: self.completed,
        candidate: candidate }, 'Updating checkpoint');

    // XXX - racy; need a lock. Don't need to update every number, just latest.
    self.checkpoint.get(function _cb(_err, _changenumber) {
        if (_err) {
            return cb(_err);
        }
        if (_changenumber >= candidate) {
            self.log.debug({ changenumber: _changenumber,
                candidate: candidate }, 'checkpoint up to date (%s >= %s)',
                _changenumber, candidate);
            return cb();
        }
        self.log.info({ changenumber: _changenumber,
            candidate: candidate }, 'updating checkpoint %s -> %s',
            _changenumber, candidate);
        self.emit('checkpoint', candidate);
        self.checkpoint.set(candidate, cb);
    });
};

// called when ready to pass/fail this changenumber.
function markComplete(obj, success, cb) {
    var self = this;
    if (self.inflight.hasOwnProperty(obj.changenumber)) {
        delete self.inflight[obj.changenumber];
        self.completed.push(obj.changenumber);
        self.log.debug({ changenubmer: obj.changenumber,
            inflight: self.inflight,
            completed: self.completed },
            'finishing changenumber');
        return self.updateCheckpoint(cb);
    } else {
        // Can't happen.
        self.log.fatal({ obj: obj },
            'Called markComplete without changenumber');
        return cb(new Error('Called markComplete without changenumber'));
    }
}

// 'Fail' at this point indicates that a required write has failed,
// and it is unsafe to persist any changenumber >= the failed one.
// Leaving the changenumber in 'inflight' (i.e., no direct action)
// will ensure we don't update unsafely.
ChangenumberStartStream.prototype.fail =
function fail(err, obj, cb) {
    this.log.warn({ err: err, obj: obj }, 'changenumber failed');
};

// obj is a changelog, we want to attach a function to it that
// will call back to this object and mark it as finished.
// we also want to expose a function on the object that
// can mark it as failed/unfinished/resumed?
ChangenumberStartStream.prototype._transform =
function _transform(obj, _, cb) {
    if (obj.hasOwnProperty('changenumber')) {
        obj.markComplete = markComplete.bind(this);
        this.inflight[obj.changenumber] = true;
    }
    this.push(obj);
    cb();
};
