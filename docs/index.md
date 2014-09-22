---
title: Minnow: the storage utilization agent
markdown2extras: tables, code-friendly
apisections:
---
<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Minnow

This is the (short) documentation for Minnow.  As there is no API, etc., for
Minnow, this documentation exists to tell you what it is, and how to configure
it.

# Overview

Minnow is a small agent that runs on every storage node in the mantaverse
(specifically in the mako zone), that literally just runs `statvfs` on a timer
and writes that data back to a pre-ordained place in Moray.  It does some
trivial math (i.e., calculating %free), so that we can leverage Postgres index
searches elsewhere, but that's about it.

The record it writes, for completeness, looks like this:

    {
          "availableMB": 8288,
          "percentUsed": 20,
          "filesystem": "/",
          "statvfs": {
            "bsize": 131072,
                "frsize": 512,
                "blocks": 21195358,
                "bfree": 16975326,
                "bavail": 16975326,
                "files": 17069136,
                "ffree": 16975326,
                "favail": 16975326,
                "fsid": 23658500,
                "basetype": "zfs",
                "flag": 4,
                "namemax": 255,
                "fstr": ""
          },
          "timestamp": 1343818838541,
          "hostname": "gibson.local.test.coal",
          "datacenter": "coal",
          "server_uuid": "e05a6490-bcad-11e1-afa7-0800200c9a66",
          "zone_uuid": "e91001d0-bcad-11e1-afa7-0800200c9a66",
          "manta_compute_id": "compute-1",
          "manta_storage_id": "storage-1"
    }

So basically we write what the host is, what the freespace looks like, and then
purely for posterity, what the raw statvfs output was.  That's pretty much
the minnow.

# Configuration

    {
        "moray": {
            "bucket": {
                "name": "manta_storage",
                "index": {
                    "hostname": { "type": "string" },
                    "availableMB": { "type": "number" },
                    "percentUsed": { "type": "number" },
                    "server_uuid": { "type": "string" },
                    "timestamp": { "type": "number" },
                    "zone_uuid": { "type": "string" },
                    "manta_compute_id": { "type": "string" },
                    "manta_storage_id": { "type": "string" }
                }
            },
            "connectTimeout": 200,
            "retry": {
                "retries": 2,
                "minTimeout": 500
            },
            "host": "127.0.0.1",
            "port": 2020
        },
        "datacenter": "coal",
        "domain": "test.coal",
        "objectRoot": "/",
        "server_uuid": "e05a6490-bcad-11e1-afa7-0800200c9a66",
        "zone_uuid": "e91001d0-bcad-11e1-afa7-0800200c9a66",
        "manta_compute_id": "compute-1",
        "manta_storage_id": "storage-1",
        "interval": 5000
    }

Which specs out what the schema looks like for the bucket in moray (this is
written at startup each time), and who/what this instance is.
