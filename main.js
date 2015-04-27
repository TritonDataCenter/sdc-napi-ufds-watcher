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
var mod_bunyan = require('bunyan');
var mod_dashdash = require('dashdash');
var mod_path = require('path');
var restifySerializers = require('restify').bunyan.serializers;
var sdcSerializers = require('sdc-bunyan-serializers');
var UFDS = require('ufds');
var util = require('util');
var vstream = require('vstream');

// pipeline
var ChangelogStream = require('./lib/changelog-stream');
var Checkpoint = require('./lib/checkpoint');
var UfdsUserStream = require('./lib/ufds-user-stream');
var UfdsFilterStream = require('./lib/ufds-filter-stream');
var ChangenumberStartStream = require('./lib/changenumber-start-stream');
var NapiFabricSetupStream = require('./lib/default-fabric-setup');
var UfdsWriteStream = require('./lib/ufds-write-stream');
var ChangenumberFinishStream = require('./lib/changenumber-finish-stream');


// --- Globals

var ME = mod_path.basename(process.argv[1]);
var query = '(&(changetype=add)(targetdn=uuid=*)' +
    '(targetdn=*ou=users, o=smartdc))';
var SMF_EXIT_NODAEMON = 94;


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
        serializers: sdcSerializers.extend(restifySerializers)
    });

    if (!conf.overlay.enabled) {
        conf.log.fatal('Fabric support not enabled, exiting.');
        process.exit(SMF_EXIT_NODAEMON);
    }

    var changenumber = 0;
    var checkpoint;
    var streams;

    var ufdsClient = new UFDS(conf.ufds);
    ufdsClient.once('connect', function () {
        conf.log.info('UFDS: connected');

        ufdsClient.removeAllListeners('error');
        ufdsClient.on('error', function (err) {
            conf.log.error(err, 'UFDS: unexpected error');
        });

        ufdsClient.on('close', function () {
            conf.log.warn('UFDS: disconnected');
            closePipeline();
            setImmediate(function () {
                openPipeline();
            });
        });

        ufdsClient.on('connect', function () {
            conf.log.info('UFDS: reconnected');
        });

        openPipeline();
    });

    ufdsClient.once('error', function (err) {
        conf.log.error(err, 'UFDS unable to connect');
    });

    function closePipeline() {
        streams.forEach(function (stream) {
            if (stream.hasOwnProperty('close')) {
                stream.close();
            }
        });
    }

    function openPipeline() {
        checkpoint = new Checkpoint({
            log: conf.log,
            ufdsClient: ufdsClient,
            url: conf.ufds.url,
            queries: [],
            dn: conf.checkpointDn,
            component: 'ufdsWatcher'
        });
        checkpoint.init(function (err, cn) {
            conf.log.info({ changenumber: cn }, 'Initialized checkpoint');

            if (conf.hasOwnProperty('changenumber')) {
                changenumber = conf.changenumber;
            } else {
                changenumber = Math.max(cn, changenumber);
            }
            createPipeline();
        });
    }

    function createPipeline() {

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

        // filters based on obj.user, no side-effects.
        var ufs = new UfdsFilterStream({
            log: conf.log,
            filter: function (user, cb) {
                if (!user.dclocalconfig ||
                    !user.dclocalconfig.defaultFabricSetup) {
                    return cb(null, true);
                }
                return cb(null, false);
            }
        });

        // tracks changenumbers that we're going to attempt to process;
        // binds markComplete function to the object.
        var cns = new ChangenumberStartStream({
            log: conf.log,
            checkpoint: checkpoint
        });

        // finds/creates a fabric vlan/overlay
        var fss = new NapiFabricSetupStream({
            log: conf.log,
            napi: conf.napi,
            defaults: conf.overlay.defaults
        });
        fss.on('failure', cns.fail);

        // updates dclocalconfig in UFDS to indicate we've set up an overlay.
        var uws = new UfdsWriteStream({
            log: conf.log,
            ufds: ufdsClient,
            datacenter_name: conf.datacenter_name
        });
        uws.on('failure', cns.fail);

        // marks completed records
        var cnf = new ChangenumberFinishStream({
            log: conf.log
        });
        cnf.on('checkpoint', function updateCheckpoint(cp) {
            changenumber = cp;
        });

        streams = [cls, uus, ufs, cns, fss, uws, cnf];
        var altPipe = new vstream.PipelineStream({
            streams: streams,
            streamOptions: { objectMode: true }
        });

        altPipe.on('error', function (err) {
            conf.log.error({
                err: err
            }, 'Pipeline error. Restarting at changenumber %s.', changenumber);
        });
    }
}
main();
