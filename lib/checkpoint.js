/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

module.exports = Checkpoint;

var assert = require('assert-plus');
var ldapjs = require('ldapjs');
var util = require('util');
var crypto = require('crypto');
var sprintf = require('util').format;

/**
 * The checkpoint Object used to store the latest consumed change numbers from
 * the remote url. The checkpoint is stored in LDAP, and represents the last
 * changelog number replicated from the remote LDAP server.
 *
 * Create this object as follows:
 *
 *  var checkpoint = new Checkpoint(opts);
 *  checkpoint.init(function (err, changenumber) { ... });
 *
 */
function Checkpoint(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufdsClient, 'opts.ufdsClient');
    assert.string(opts.url, 'opts.url');
    assert.arrayOfString(opts.queries, 'opts.queries');
    assert.string(opts.component, 'opts.component');

    /**
     * The local UFDS client
     */
    this.ufdsClient = opts.ufdsClient;
    this.log = opts.log;

    /**
     * The remote UFDS URL
     */
    this.url = opts.url;

    /**
     * The remote UFDS replication queries
     */
    this.queries = opts.queries;

    /**
     * The optional 'component'
     */
    this.component = opts.component;

    /**
     * The search DN
     */
    if (opts.dn === '') {
        this.srch_dn = 'o=smartdc';
    } else {
        this.srch_dn = opts.dn;
    }
    console.error('XXX - srch_dn = ', this.srch_dn);
}

/**
 * Initializes a new checkpoint entry
 * The uid and query properties are no longer used but they are required in
 * the schema so we continue to provide values for compatibility.
 */
Checkpoint.prototype.newEntry = function (uid) {
    var entry = {
        objectclass: 'sdcreplcheckpoint',
        changenumber: 0,
        uid: uid,
        url: this.url,
        query: this.queries,
        component: this.component
    };

    return entry;
};



/**
 * Initializes the checkpoint object, and sets the checkpoint to 0 if no
 * checkpoint exists.
 */
Checkpoint.prototype.init = function (callback) {

    this.log.debug('Initializing checkpoint for url %s', this.url);

    this.get(function (err, changenumber) {
        if (err) {
            return callback(err);
        }

        return callback(null, changenumber);
    });
};



/**
 * Gets the current checkpoint
 * @param {function} callback : function(err, changenumber).
 */
Checkpoint.prototype.get = function get(callback) {
    assert.func(callback, 'callback');

    var self = this;
    var srch_opts = {
        scope: 'sub',
        filter: sprintf('(&(objectclass=sdcreplcheckpoint)(url=%s))',
            this.url)
    };

    self.ufdsClient.search(self.srch_dn, srch_opts,
        function onCheckpoint(err, res) {

        if (err) {
            return callback(err);
        }

        // expecting at-most-one
        if (res.length > 1) {
            var dnStr = res.reduce(function (acc, e) {
                return acc + e.object.dn
            }, '');
            var msg = util.format('Multiple entries for same url %s: %s',
                self.url, dnStr);
            var _err = new Error(msg);
            self.log.error({ err: _err }, msg);
            return callback(_err);
        }

        if (res.length === 1) {
            var changenumber = res[0].changenumber;
            var dn = res[0].dn;
            self.log.debug({ entry: res[0] }, 'Found changenumber %s in dn %s',
                changenumber, dn);
            self.dn = dn;
            changenumber = parseInt(changenumber, 10);
            return callback(null, changenumber);
        }

        var urlHash = crypto.createHash('md5')
            .update(self.url).digest('hex');
        var entry = self.newEntry(urlHash);

        self.dn = 'uid=' + urlHash + ', ' + self.srch_dn;

        self.log.debug({ entry: entry, dn: self.dn }, 'No checkpoint for url %s, init dn %s to %s',
            self.url, self.dn, entry.changenumber);

        self.ufdsClient.add(self.dn, entry, function (addErr) {
            if (addErr) {
                self.log.error({ err: addErr, entry: entry, dn: self.dn },
                    'Failed to init checkpoint at dn %s', self.dn);
                return callback(addErr);
            }
            return callback(null, entry.changenumber);
        });
    });
};



/**
 * Sets the current checkpoint
 * @param {int} changenumber : the changnumber to set the checkpoint to.
 * @param {function} callback : function().
 */
Checkpoint.prototype.set = function (changenumber, callback) {
    var self = this;

    var change = new ldapjs.Change({
        type: 'replace',
        modification: {
            changenumber: changenumber
        }
    });

    self.ufdsClient.modify(self.dn, change, function (err, res) {
        if (err) {
            self.log.fatal(err,
                'Unable to set checkpoint to changenumber %s',
                changenumber);
            return callback(err);
        }

        self.log.debug('Checkpoint set to %s', changenumber);
        return callback();
    });
};
