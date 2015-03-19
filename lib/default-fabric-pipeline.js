/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/**
 * Encapsulates the pipeline to set up a default fabric
 */

var assert = require('assert-plus');
var ChangelogStream = require('./lib/changelog-stream');
var mod_bunyan = require('bunyan');
var UfdsUserStream = require('./lib/ufds-user-stream');
var util = require('util');
var Writable = require('stream').Writable;

// --- Exports

function DefaultFabricPipeline(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufds, 'opts.ufds');
    assert.object(opts.napi, 'opts.napi');
    assert.object(opts.defaults, 'opts.default');

    var query = '(&(changetype=add)(targetdn=uuid=*)(targetdn=*ou=users, o=smartdc))';

    this.changenumber = opts.changenumber || 0;

    this.cls = new ChangelogStream({
        log: opts.log,
        ufds: opts.ufds,
        changenumber: changenumber,
        query: query
    });

    this.uus = new UfdsUserStream({
        log: opts.log,
        ufds: opts.ufds
    });

    this.fss = new NapiFabricSetupStream({
        log: opts.log,
        napi: opts.napi,
        defaults: opts.defaults
    });

    this.uws = new UfdsFabricSetupStream({
        log: opts.log,
        ufds: opts.ufds
    });

    this.fin = new FinishStream();

    fin.on('changenumber', function(cn) {
        if (cn > this.changenumber) {
            this.changenumber = cn;
        }
    });

    // if any of these emit error, should we handle it here, or pass it out?
    // UFDS errors should be passed out here? or retried in the stream?
}
module.exports = DefaultFabricPipeline;

// export some sort of init/connect method? Should it expose a stream interface?
// that might work, since 'pipe' returns the target stream. Or just leave it at this.
