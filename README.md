<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Minnow

Repository: <git@git.joyent.com:minnow.git>
Browsing: <https://mo.joyent.com/minnow>
Who: Mark Cavage
Docs: <https://mo.joyent.com/docs/minnow>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repo contains Minnow, the storage utilization agent.

# Development

You'll need a Moray instance up and running first. Go see the Moray docs on how
to do that. Once you have it up, you can probably just use the
`config.coal.json` file located in `./etc`.  Run `make` to bring in all the
dependencies, then:

    . ./env.sh
	node main.js -vv -f ./etc/config.coal.json | bunyan

And you should see minnow heartbeating statvfs output to moray.  You should be
able to kill off the remote moray instance and restart it to see reconnect logic
working.
