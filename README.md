<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2015, Joyent, Inc.
-->

# sdc-napi-ufds-watcher

This repository is part of the Joyent SmartDataCenter project (SDC). For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

This is a daemon that watches [UFDS](https://github.com/joyent/sdc-ufds) for
new users and adds default fabric networks and VLANs to
[NAPI](https://github.com/joyent/sdc-napi) for those users.


## Development

To run tests:

    make test

To run style and lint checks:

    make check

To run all checks and tests:

    make prepush


## License

sdc-napi-ufds-watcher is licensed under the
[Mozilla Public License version 2.0](http://mozilla.org/MPL/2.0/).
See the file LICENSE.
