/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * A stream interface to the UFDS changelog; provides a customisable filter.
 */


var assert = require('assert-plus');
var Readable = require('stream').Readable;
var util = require('util');
var uuid = require('node-uuid');


// --- Globals

var sprintf = util.format;

var CHANGELOG_DN = 'cn=changelog';
// XXX - make an opt, at least (&(changenumber>=%d)($QUERY_OPTS))
// then can be made generic; maybe part of ufds? on its own? Can we
// support a general query?
var FILTER = '(&(changenumber>=%d)%s)';


// --- API

function ChangelogStream(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.number(opts.interval, 'opts.interval');
    assert.object(opts.ufds, 'opts.ufds');
    assert.optionalNumber(opts.changenumber, 'opts.changenumber');
    assert.optionalString(opts.query, 'opts.query');

    Readable.call(this, {
        objectMode: true
    });

    this.log = opts.log.child({component: 'ChangelogStream'}, true);

    this.changenumber = opts.changenumber || 0;
    this.ufdsClient = opts.ufds;
    this.interval = opts.interval;
    this.timeout = this.interval / 2;
    this.polling = false;
    this.query = opts.query;
}
util.inherits(ChangelogStream, Readable);
module.exports = ChangelogStream;


ChangelogStream.prototype.close = function close() {
    this.log.debug('close: entered');
    clearTimeout(this.pollTimer);

    if (this.dead) {
        setImmediate(this.emit.bind(this, 'close'));
        return;
    }

    this.dead = true;
    this.push(null);
    this.emit('close');

    this.log.debug('close: done');
};


ChangelogStream.prototype._read = function _read() {
    this._poll();
};


ChangelogStream.prototype._poll = function _poll() {
    clearTimeout(this.pollTimer);
    if (this.dead || !this.ufdsClient.connected || this.polling) {
        return;
    }

    var log = this.log;
    var opts = {
        scope: 'sub',
        filter: sprintf(FILTER, this.changenumber + 1, this.query),
        sizeLimit: 1000
    };
    var self = this;
    var searchTimeout = setTimeout(function onTimeout() {
        if (self.dead) {
            return;
        }

        log.error('_poll: ldap_search timeout');
        self.pollTimer = setTimeout(self._poll.bind(self), self.interval);
    }, this.timeout);

    log.debug('_poll: entered');

    this.polling = true;
    this.ufdsClient.search(CHANGELOG_DN, opts, function onSearch(err, res) {
        self.polling = false;
        clearTimeout(searchTimeout);

        if (err) {
            log.error(err, '_poll: ldap_search failed');
            self.pollTimer = setTimeout(self._poll.bind(self), self.interval);
            return;
        }

        var found = res.length > 0;
        if (!found) {
            self.emit('fresh');
            self.pollTimer = setTimeout(self._poll.bind(self), self.interval);
            return;
        }

        for (var i = 0; i < res.length; i++) {
            var entry = res[i];
            var changenumber = parseInt(entry.object.changenumber, 10);
            if (changenumber > self.changenumber) {
                self.changenumber = changenumber;
            } else {
                log.fatal('_poll: changenumber out of order. ' +
                    'expected changenumber > %s, but got %s',
                    self.changenumber, changenumber);
                self.emit('error', 'changenumber out of order');
            }

            // If backpressure is applied, discard the rest of the entries and
            // we'll start from there next time poll() is called.
            entry.object.requestId = uuid.v4();
            log.debug({ entry: entry.object },
                'Pushing changelog with cn %s, requestId %s',
                changenumber, entry.object.requestId);
            if (!self.push(entry.object)) {
                self.polling = false;
                break;
            }
        }

        // Poll again immediately if we were able to successfully push all of
        // the entries we found, since there are probably more entries.
        if (self.polling) {
            self._poll();
        }
    }, /* noCache = */ true);
};


ChangelogStream.prototype.toString = function toString() {
    var str = '[object ChangelogStream <';
    str += 'changenumber=' + this.changenumber + ', ';
    str += 'interval=' + this.interval + ', ';
    str += 'url=' + this.ufdsClient.url;
    str += '>]';

    return (str);
};



// --- Tests

function test() {
    var bunyan = require('bunyan');
    var dashdash = require('dashdash');
    var path = require('path');

    var options = [
        {
            names: ['help', 'h'],
            type: 'bool',
            help: 'Print this help and exit.'
        },
        {
            names: ['config', 'f'],
            type: 'string',
            env: 'MAHI_CONFIG',
            helpArg: 'PATH',
            default: path.resolve(__dirname, '../../etc/mahi.json'),
            help: 'configuration file with ufds and redis config settings'
        },
        {
            names: ['changenumber', 'c'],
            type: 'number',
            helpArg: 'CHANGENUMBER',
            default: 0,
            help: 'changenumber to start at'
        },
        {
            names: ['poll', 'p'],
            type: 'bool',
            help: 'continue polling after no new entries are found'
        },
        {
            names: ['ufds-url'],
            type: 'string',
            env: 'MAHI_UFDS_URL',
            helpArg: 'URL',
            help: 'ufds url (overrides config)'
        }
    ];

    var parser = dashdash.createParser({options: options});
    var opts;
    try {
        opts = parser.parse(process.argv);
    } catch (e) {
        console.error('error: %s', e.message);
        process.exit(1);
    }

    if (opts.help) {
        var help = parser.help().trimRight();
        console.log('usage: \n' + help);
        process.exit(0);
    }

    var config = require(path.resolve(opts.config));
    var ufdsConfig = config.ufds || {
        bindDN: 'cn=root',
        bindCredentials: 'secret',
        interval: 5000,
        maxConnections: 1,
        log: log,
        tlsOptions: {
            rejectUnauthorized: false
        },
        url: 'ldap://localhost:1389'
    };

    ufdsConfig.url = opts['ufds-url'] || ufdsConfig.url;

    var log = bunyan.createLogger({
        level: process.env.LOG_LEVEL || 'info',
        name: 'changelog_test',
        stream: process.stdout,
        src: true
    });

    var stream = new ChangelogStream({
        log: log,
        ufds: ufdsConfig,
        changenumber: opts.changenumber
    });

    stream.on('data', function (obj) {
        console.log(JSON.stringify(obj));
    });

    stream.on('fresh', function () {
        if (opts.poll) {
            console.warn('no new entries (CTRL+C to exit)');
        } else {
            process.exit(0);
        }
    });
}

if (require.main === module) {
    test();
}
