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
var Transform = require('stream').Transform;
var util = require('util');

function ChangenumberFinishStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    Transform.call(this, { objectMode: true });

    var self = this;

    self.log = opts.log.child({ component: 'ChangenumberFinishStream' }, true);
}
util.inherits(ChangenumberFinishStream, Transform);
module.exports = ChangenumberFinishStream;

ChangenumberFinishStream.prototype._transform =
function _transform(obj, _, cb) {
    var self = this;
    if (obj.hasOwnProperty('markComplete') &&
        typeof obj.markComplete === 'function') {
        obj.markComplete(function (err) {
            if (err) {
                self.log.error({ err: err, obj: obj },
                    'Failed to mark %o as completed', obj);
            }
        });
    }
    cb();
}
