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
// var DefaultFabricPipeline = require('./lib/default-fabric-pipeline');
var mod_bunyan = require('bunyan');
var mod_dashdash = require('dashdash');
var mod_path = require('path');
var UFDS = require('ufds');
var util = require('util');
var vstream = require('vstream');

// streams
var ChangelogStream = require('./lib/changelog-stream');
var UfdsUserStream = require('./lib/ufds-user-stream');
var UfdsFilterStream = require('./lib/ufds-filter-stream');
var NapiFabricSetupStream = require('./lib/default-fabric-setup');
var UfdsWriteStream = require('./lib/ufds-write-stream');
var Writable = require('stream').Writable;



// --- Globals



var ME = mod_path.basename(process.argv[1]);


function TrivialStream(opts) {
    Writable.call(this, {
        objectMode: true
    });
    if (opts && opts.func) {
        this.func = opts.func;
    }
}
util.inherits(TrivialStream, Writable);

TrivialStream.prototype._write = function (thing) {
    if (thing.foo && typeof (thing.foo) === 'function') {
        thing.foo();
    }
    if (this.func) {
        this.func(thing);
    } else {
        console.log('STREAMED A THING', util.inspect(thing));
    }
};

// --- Exports

/**
 * Instantiates streams to:
 *   1) stream the changelog
 *   2) get actual user objects from those changes
 *   3) filter out certain users
 *   4) set up a default overly vlan & network for the user
 *   5) record that we did that
 */
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
            util.format('usage: %s [OPTIONS]', ME),
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
    // TODO sapi tunable? config addition?
    conf.ufds.interval = 10000;

    var query = '(&(changetype=add)(targetdn=uuid=*)' +
        '(targetdn=*ou=users, o=smartdc))';
    var changenumber = 0;

    // TODO - update opts here for retries, etc.
    var ufdsClient = new UFDS(conf.ufds);
    // on first connect
    ufdsClient.once('connect', function () {
        conf.log.info('UFDS: connected');

        ufdsClient.removeAllListeners('error');
        ufdsClient.on('error', function (err) {
            conf.log.error(err, 'UFDS: unexpected error');
        });

        ufdsClient.on('close', function () {
            conf.log.warn('UFDS: disconnected');
        });

        ufdsClient.on('connect', function () {
            conf.log.info('UFDS: reconnected');
        });
    });

    ufdsClient.once('error', function (err) {
        conf.log.error(err, 'UFDS unable to connect');
    });


    // connects to local ufds & streams changelog entries,
    // emits changelogs of users & subusers
    var cls = new ChangelogStream({
        log: conf.log,
        interval: conf.ufds.interval,
        ufds: ufdsClient,
        changenumber: changenumber,
        query: query
    });

    // transforms changelog entries to:
    // obj {
    //     user: UFDSClient User object,
    //     changenumber: original changenumber
    // }
    var uus = new UfdsUserStream({
        log: conf.log,
        ufds: ufdsClient
    });

    // filters based on obj.user, does not alter objects.
    var ufs = new UfdsFilterStream({
        log: conf.log,
        filter: function (user, cb) {
            if (!user.dclocalconfig || !user.dclocalconfig.defaultFabricSetup) {
                return cb(null, true);
            }
            return cb(null, false);
        }
    });

    // creates overlay network per config defaults, adds property to obj:
    // {
    //     defaultNetwork: UUID
    // }
    var fss = new NapiFabricSetupStream({
        log: conf.log,
        napi: conf.napi,
        defaults: conf.defaults
    });

    // updates dclocalconfig in UFDS to indicate we've set up an overlay.
    // updates obj.user.
    var uws = new UfdsWriteStream({
        log: conf.log,
        ufds: ufdsClient,
        datacenter_name: conf.datacenter_name
    });

    var ts = new TrivialStream();

    var altPipe = new vstream.PipelineStream({
        streams: [cls, uus, ufs, fss, uws],
        streamOptions: { objectMode: true }
    });

    altPipe.on('error', function (err) {
        conf.log.error(err, 'Pipeline error, what now? Make a new pipe?');
    });

    altPipe.pipe(ts);

    // var pipeline = new DefaultFabricPipeline(opts);

    // emits 'finished' event with latest changenumber.
    // var fin = new FinishStream();

    // error handling needs to be at this point.
    // if any of them close, they'll all close; does this emit close over
    // and over?
    // or is 'err' better here? Both might apply.
    // Either way, we want to have something like:

    // function makePipe() {
    //     var restart = once(restartPipe);

    //     // new streams each time, use the changenumber
    //     //we maintain from the final one.

    //     cls.on('close', restart);
    //     uus.on('close', restart);
    //     // ...
    //     var pipe = cls.pipe(uus).pipe(ts)
    // }

    // function restartPipe() {
    //     // remove all close/error listeners, because they were once'd
    //     // set them up again & refire.
    //     cls.removeAllListeners('close');
    //     // ...
    //     makePipe();
    // }
}
main();
