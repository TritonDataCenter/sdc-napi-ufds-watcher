/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

// create UFDS client

// create new user info
// create subuser info (given user)
// create user *with* default stuff
// create disabled user
// get current changenumber (to start stream at desired point)


var assert = require('assert');
var bunyan = require('bunyan');
var util = require('util');

var NAMES=[];


// create a user, bind on the 'destroy' and 'create' methods,
// data in the 'data' member.
function generateUser(fields) {
    var user = {
        dn: 'uuid=a820621a-5007-4a2a-9636-edde809106de, ou=users, o=smartdc',
        object: {
            login: 'unpermixed',
            uuid: 'a820621a-5007-4a2a-9636-edde809106de',
            userpassword: 'FL8xhOFL8xhO',
            email: 'postdisseizor@superexist.com',
            cn: 'Judophobism',
            sn: 'popgun',
            company: 'butterflylike',
            address: ['liltingly, Inc.',
            '6165 pyrophyllite Street'],
            city: 'benzoylation concoctive',
            state: 'SP',
            postalCode: '4967',
            country: 'BAT',
            phone: '+1 891 657 5818',
            objectclass: 'sdcperson'
        }
    }



    return user;
}


// dn: uuid=390c229a-8c77-445f-b227-88e41c2bb3cf, ou=users, o=smartdc
// changetype: add
// cn: Jerry Jelinek
// company: Joyent
// email: jerry.jelinek@joyent.com
// givenname: Jerry
// login: jjelinek
// objectclass: sdcperson
// phone: 17194956686
// sn: Jelinek
// uuid: 390c229a-8c77-445f-b227-88e41c2bb3cf
// pwdchangedtime: 1366098656879
// userpassword: M@nt@R@ys&r3Th3C00l3st!
// approved_for_provisioning: true
// created_at: 1366382418140
// updated_at: 1366382418140

// # sdcaccountuser add
// dn: uuid=3ffc7b4c-66a6-11e3-af09-8752d24e4669, uuid=390c229a-8c77-445f-b227-88e41c2bb3cf, ou=users, o=smartdc
// changetype: add
// login: subuser
// email: subuser@example.com
// uuid: 3ffc7b4c-66a6-11e3-af09-8752d24e4669
// userpassword: secret123
// objectclass: sdcperson

// dn -> new uuid
// login -> random
// email -> random@example.com
//
// subusers?
//
//

module.exports = {

}
