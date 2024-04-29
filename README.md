<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
    Copyright 2024 MNX Cloud, Inc.
-->

# manta-minnow

This repository is part of the Triton Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/TritonDataCenter/manta) project page.

This repo contains Minnow, the storage utilization agent.

## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md)
for details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.

## Development

You'll need a Moray instance up and running first. Go see the Moray docs on how
to do that. Once you have it up, you can probably just use the
`config.coal.json` file located in `./etc`.  Run `make` to bring in all the
dependencies, then:

    . ./env.sh
    node main.js -vv -f ./etc/config.coal.json | bunyan

And you should see minnow heartbeating statvfs output to moray.  You should be
able to kill off the remote moray instance and restart it to see reconnect logic
working.
