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
var fmt = require('util').format;
var mod_bunyan = require('bunyan');
var mod_dashdash = require('dashdash');
var mod_path = require('path');
var mod_watcher = require('./lib/watcher');



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

    mod_watcher.create(conf, function (err, watcher) {
        if (err) {
            throw err;
        }

        log.debug('watcher created');
        watcher.watch();
    });
}

main();
