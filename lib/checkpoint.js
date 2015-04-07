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
var EventEmitter = require('events').EventEmitter;
var ldapjs = require('ldapjs');
var util = require('util');
var crypto = require('crypto');
var sprintf = require('util').format;

/**
 * The checkpoint Object used to store the latest consumed change numbers from
 * the remote url. The checkpoint is stored in LDAP, and represents the last
 * changelog number replicated from the remote LDAP server. Emits an 'init'
 * event when instantiated.
 *
 * Create this object as follows:
 *
 *  var checkpoint = new Checkpoint();
 *  checkpoint.once('init', function(cn) {
 *      console.log('checkpoint has been initialized with changnumber', cn);
 *  })
 *  checkpoint.init();
 *
 */
function Checkpoint(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.ufdsClient, 'opts.ufdsClient');
    assert.string(opts.url, 'opts.url');
    assert.arrayOfString(opts.queries, 'opts.queries');

    EventEmitter.call(this);

    /**
     * The local UFDS client
     */
    this.client = opts.client;
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
     * The search DN
     */
    if (opts.dn === '') {
        this.srch_dn = 'o=smartdc';
    } else {
        this.srch_dn = opts.dn;
    }
}

util.inherits(Checkpoint, EventEmitter);



/*
 * Accessor for the localUfds/client instance
 */
Checkpoint.prototype.client = function (callback) {
    var replicator = this.replicator;

    return replicator.ensureLocalUfds(function () {
        return callback(replicator.localUfds);
    });
};



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
        component: 'ufdsWatcher'
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
        filter: sprintf('(&(objectclass=sdcreplcheckpoint)(url=%s))', this.url)
    };

    self.client(function (ufds) {
        ufds.search(self.srch_dn, srch_opts, onCheckpoint);

        function onCheckpoint(err, res) {
            if (err) {
                return callback(err);
            }

            res.on('searchEntry', function (entry) {
                var fnd_dn = entry.object.dn;

                if (self.dn) {
                    var msg = util.format(
                        'Multiple entries for the same url, dn %s',
                        fnd_dn);
                    var _err = new Error(msg);
                    self.log.error({ err: _err, dn: fnd_dn }, msg);
                    return callback(_err);
                }

                var changenumber = entry.object.changenumber;
                self.log.debug('Got changenumber %s on dn %s', changenumber,
                   fnd_dn);

                self.dn = fnd_dn;
                changenumber = parseInt(changenumber, 10);
                return callback(null, changenumber);
            });

            res.on('end', function () {
                if (!self.dn) {
                    var urlHash = crypto.createHash('md5')
                        .update(self.url).digest('hex');
                    var entry = self.newEntry(urlHash);

                    self.dn = 'uid=' + urlHash + ', ' + self.srch_dn;

                    self.log.debug('No checkpoint, initializing dn %s to 0',
                        self.dn);
                    ufds.add(self.dn, entry, function (err2, res2) {
                        if (err2) {
                            return callback(err2);
                        }

                        return callback(null, 0);
                    });
                }
            });

            res.on('error', function (err2) {
                self.log.fatal(err2, 'Unable to fetch checkpoint');
                return callback(err2);
            });
        }
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

    self.client(function (ufds) {
        ufds.modify(self.dn, change, function (err, res) {
            if (err) {
                self.log.fatal(err,
                    'Unable to set checkpoint to changenumber %s',
                    changenumber);
                return callback(err);
            }

            self.log.debug('Checkpoint set to %s', changenumber);
            return callback();
        });
    });
};
