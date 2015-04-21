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
    assert.object(opts.ufdsClient, 'opts.ufdsClient');
    assert.string(opts.url, 'opts.url');
    assert.string(opts.checkpointDn, 'opts.checkpointDn');
    assert.string(opts.component, 'opts.component');

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'ChangenumberStartStream' }, true);

    self.ufdsClient = opts.ufdsClient;

    self.inflight = {};
    self.updates = [];

    self.checkpoint = new Checkpoint({
        log: self.log,
        ufdsClient: self.ufdsClient,
        url: opts.url,
        queries: [],
        dn: opts.checkpointDn,
        component: opts.component
    });
    self.checkpoint.init(function (err, cn) {
        self.log.info({ changenumber: cn }, 'Initialized checkpoint at %s',
            cn);
    });
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

    self.log.debug({ inflight: self.inflight, completed: self.completed },
        'Updating checkpoint');
    var lowestInFlight = self.inflight.sort(function (a, b) {
        return a > b;
    })[0];
    var candidate = self.completed.reduce(function (acc, e) {
        if (e > acc && e < lowestInFlight) {
            acc = e;
        }
        return acc;
    }, 0);

    self.checkpoint.get(function _cb(_err, _changenumber) {
        if (_err) {
            return cb(_err);
        }
        if (_changenumber < candidate) {
            self.log.debug({ changenumber: _changenumber,
                candidate: candidate }, 'checkpoint %d already past %d',
                _changenumber, candidate);
            return cb();
        }
        self.log.info({ changenumber: _changenumber,
            candidate: candidate }, 'updating checkpoint %s -> %s',
            _changenumber, candidate);
        self.emit('checkpoint', _changenumber);
        self.checkpoint.set(_changenumber, cb);
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
            'finishing changenumber %s', obj.changenumber);
        return self.updateCheckpoint(cb);
    } else {
        // XXX - what would cause this?
        return cb(new Error('wtf'));
    }
}

ChangenumberStartStream.prototype.fail =
function fail(obj, cb) {
    this.log.debug({ obj: obj }, 'changenumber %s failed to complete',
        obj.changenumber);
    // XXX - what to do about failures?
    // definitely log them.
    // persist them? amon alarm? Failures that aren't errors
    // should probably be continued over.
};

// obj is a changelog, we want to attach a function to it that
// will call back to this object and mark it as finished.
// we also want to expose a function on the object that
// can mark it as failed/unfinished/resumed?
ChangenumberStartStream.prototype._transform =
function _transform(obj, _, cb) {
    obj.markComplete = markComplete.bind(this);
    this.push(obj);
    cb();
};
