/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * UFDS user watcher: main entry point
 */

var assert = require('assert-plus');
var ChangelogStream = require('./lib/changelog-stream');
var fmt = require('util').format;
var mod_bunyan = require('bunyan');
var mod_dashdash = require('dashdash');
var mod_path = require('path');
var UfdsUserStream = require('./lib/ufds-user-stream');
var util = require('util');
var Writable = require('stream').Writable;




// --- Globals



var ME = mod_path.basename(process.argv[1]);



// --- Exports



function main() {
    var opts;
    var options = [
        {
            names: [ 'file', 'f' ],
            type: 'string',
            help: 'Configuration file to use.'
        },
        {
            names: [ 'help', 'h' ],
            type: 'bool',
            help: 'Print help and exit.'
        }
    ];
    var parser = mod_dashdash.createParser({options: options});

    try {
        opts = parser.parse(process.argv);
    } catch (parseErr) {
        console.error('%s: error: %s', ME, parseErr.message);
        process.exit(1);
    }

    if (opts.help) {
        console.log([
            fmt('usage: %s [OPTIONS]', ME),
            '',
            'Options:',
            parser.help({includeEnv: true}).trimRight()
        ].join('\n'));
        process.exit(1);
    }

    if (!opts.file) {
        console.error('--file option is required!');
        process.exit(1);
    }

    var conf = require(mod_path.resolve(opts.file));
    conf.log = mod_bunyan.createLogger({
        name: 'napi-ufds-watcher',
        level: conf.logLevel || 'debug',
        serializers: mod_bunyan.stdSerializers
    });

    // prefer single copies of clients?
    // single queues for UFDS/portolan messages?
    // create streams
    // chageLogStream
    // userFilterStream to filter out fabric-setup users
    // portolan filter setup stream
    // ufds update stream
    // then pipe them all to eachother, and boom?

    // create stream, pipe to trivial writable that echos to stderr?

    function TrivialStream(opts) {
        Writable.call(this, {
            objectMode: true
        });
    }
    util.inherits(TrivialStream, Writable);
    TrivialStream.prototype._write = function(thing) {
        console.log("STREAMED A THING", util.inspect(thing));
    };

    var query = '(&(changetype=add)(targetdn=uuid=*)(targetdn=*ou=users, o=smartdc))'

    // XXX sapi tunable.
    conf.ufds.interval = 10000;

    var cls = new ChangelogStream({
        log: conf.log,
        ufds: conf.ufds,
        changenumber: 0,
        query: query
    });

    var uus = new UfdsUserStream({
        log: conf.log,
        ufds: conf.ufds
    });

    var fss = new NapiFabricSetupStream({
        log: conf.log,
        napi: conf.napi,
        defaults: conf.defaults
    });

    var uws = new UfdsFabricSetupStream({
        log: conf.log,
        ufds: conf.ufds
    });

    var ts = new TrivialStream();

    var pipe = cls.pipe(uus).pipe(ts);
}
main();
