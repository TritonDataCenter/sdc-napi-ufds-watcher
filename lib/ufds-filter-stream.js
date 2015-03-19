/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * A Transform stream to filter UFDS objects by some criteria
 */

var assert = require('assert-plus');
var Transform = require('stream').Transform;
var UFDS = require('ufds');
var util = require('util');

function UfdsFilterStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalFunc(opts.filter);

    Transform.call(this, { objectMode: true });

    this.log = opts.log.child({ component: 'UfdsUserStream'}, true);

    this.predicate = opts.filter ||
        function (x, cb) { return cb(null, true); };
}
util.inherits(UfdsFilterStream, Transform);
module.exports = UfdsFilterStream;

UfdsFilterStream.prototype._transform = function _transform(obj, _, cb) {
    var self = this;
    self.log.debug({ user: obj.user }, 'filtering user %s', obj.user.uuid);
    this.predicate(obj.user, function (err, pass) {
        if (err) {
            return cb(err);
        }
        if (!pass) {
            self.log.debug({ user: obj.user },
                'user %s dropped', obj.user.uuid);
        } else {
            self.log.debug({ user: obj.user },
                'user %s passed', obj.user.uuid);
            self.push(obj);
        }
        return cb();
    });
};
