// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var fs = require('fs');
var os = require('os');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var libmanta = require('libmanta');
var moray = require('moray');
var once = require('once');
var statvfs = require('statvfs');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: 'minnow',
    serializers: libmanta.bunyan.serializers,
    stream: process.stdout
});
var OPTIONS = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Print this help and exit.'
    },
    {
        names: ['verbose', 'v'],
        type: 'arrayOfBool',
        help: 'Verbose output. Use multiple times for more verbose.'
    },
    {
        names: ['file', 'f'],
        type: 'string',
        help: 'File to process',
        helpArg: 'FILE'
    }
];
var HEARTBEAT;
var INTERVAL;
var TIMER;



///--- CLI Functions

function configure() {
    var cfg;
    var opts;
    var parser = new dashdash.Parser({options: OPTIONS});

    try {
        opts = parser.parse(process.argv);
        assert.object(opts, 'options');
    } catch (e) {
        LOG.fatal(e, 'invalid options');
        process.exit(1);
    }

    if (opts.help) {
        console.log('usage: node main.js [OPTIONS]\n'
                    + 'options:\n'
                    + parser.help().trimRight());
        process.exit(0);
    }

    try {
        var _f = opts.file || __dirname + '/etc/config.json';
        cfg = JSON.parse(fs.readFileSync(_f, 'utf8'));
    } catch (e) {
        LOG.fatal(e, 'unable to parse %s', _f);
        process.exit(1);
    }


    if (cfg.logLevel)
        LOG.level(cfg.logLevel);

    if (opts.verbose) {
        opts.verbose.forEach(function () {
            LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
        });
    }

    if (LOG.level() <= bunyan.DEBUG)
        LOG = LOG.child({src: true});

    function assertNonEmptyString(s, label) {
        assert.string(s, label);
        if (s.length === 0) {
            LOG.fatal({
                option: label
            }, 'option is empty');
            process.exit(1);
        }
    }

    assert.object(cfg.moray, 'config.moray');
    assertNonEmptyString(cfg.moray.bucket.name, 'cfg.moray.bucket.name');
    assertNonEmptyString(cfg.datacenter, 'cfg.datacenter');
    assertNonEmptyString(cfg.domain, 'cfg.domain');
    assertNonEmptyString(cfg.objectRoot, 'cfg.objectRoot');
    assertNonEmptyString(cfg.server_uuid, 'cfg.server_uuid');
    assertNonEmptyString(cfg.zone_uuid, 'cfg.zone_uuid');
    assertNonEmptyString(cfg.manta_compute_id, 'cfg.manta_compute_id');
    assertNonEmptyString(cfg.manta_storage_id, 'cfg.manta_storage_id');

    cfg.moray.log = LOG;

    return (cfg);
}



///--- worker functions

function createMorayClient(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.optionalObject(opts.retry, 'options.retry');
    assert.func(cb, 'callback');

    cb = once(cb);

    var retry = opts.retry || {};
    retry.retries = retry.retries || Infinity;
    retry.minTimeout = retry.minTimeout || 2000;
    retry.maxTimeout = retry.maxTimeout || 120000;
    var client = moray.createClient({
        connectTimeout: opts.connectTimeout,
        log: opts.log,
        maxConnections: opts.maxConnections,
        host: opts.host,
        port: opts.port,
        retry: retry
    });

    function onConnectError(err) {
        LOG.fatal(err, 'moray: connect: failed');
        client.removeListener('connect', onConnectSetup);
        cb(err);
    }

    function onConnectSetup() {
        var bname = opts.bucket.name;
        var index = opts.bucket.index;

        client.removeListener('error', onConnectError);
        client.putBucket(bname, {index: index}, function (err) {
            if (err) {
                LOG.fatal(err, 'moray: putBucket: failed');
                cb(err);
            } else {
                LOG.info('moray: connected');
                cb(null, client);
            }
        });
    }

    client.once('connect', onConnectSetup);
    client.once('error', onConnectError);
}


function stat(filesystem, callback) {
    statvfs(filesystem, function (err, s) {
        if (err)
            return (callback(err));

        var free = s.bavail * s.frsize;
        var total = s.blocks * s.frsize;
        var used = total - free;

        var obj = {
            availableMB: Math.floor(free / 1048576),
            percentUsed: Math.ceil((used / total) * 100),
            filesystem: filesystem,
            statvfs: s,
            timestamp: Date.now()
        };

        return (callback(null, obj));
    });
}


function heartbeat(opts) {
    assert.object(opts, 'options');
    assert.string(opts.bucket, 'options.bucket');
    assert.string(opts.datacenter, 'options.datacenter');
    assert.string(opts.domain, 'options.domain');
    assert.object(opts.moray, 'options.moray');
    assert.string(opts.objectRoot, 'options.objectRoot');
    assert.string(opts.server_uuid, 'options.server_uuid');
    assert.string(opts.zone_uuid, 'options.zone_uuid');
    assert.string(opts.manta_compute_id, 'options.manta_compute_id');
    assert.string(opts.manta_storage_id, 'options.manta_storage_id');

    var key = os.hostname() + '.' + opts.domain;
    stat(opts.objectRoot, function (stat_err, stats) {
        if (stat_err) {
            LOG.error(stat_err, 'unable to call statvfs');
            return;
        }

        stats.hostname = key;
        stats.datacenter = opts.datacenter;
        stats.server_uuid = opts.server_uuid;
        stats.zone_uuid = opts.zone_uuid;
        stats.manta_compute_id = opts.manta_compute_id;
        stats.manta_storage_id = opts.manta_storage_id;

        opts.moray.putObject(opts.bucket, key, stats, function (err) {
            if (err) {
                LOG.error(err, 'moray: update failed');
                return;
            }

            LOG.info({
                bucket: opts.bucket,
                key: key,
                stats: stats
            }, 'heartbeat: complete');
        });
    });
}



///--- Mainline

(function main() {
    var cfg = configure();

    createMorayClient(cfg.moray, function (err, client) {
        assert.ifError(err); // should never return error

        LOG.info({
            bucket: cfg.moray.bucket,
            objectRoot: cfg.objectRoot
        }, 'moray setup done: starting stat daemon');

        // Set up globals so we can disable/reenable in the moray
        // connection status handlers
        INTERVAL = cfg.interval || 30000;
        HEARTBEAT = heartbeat.bind(null, {
            bucket: cfg.moray.bucket.name,
            datacenter: cfg.datacenter,
            domain: cfg.domain,
            moray: client,
            objectRoot: cfg.objectRoot,
            server_uuid: cfg.server_uuid,
            zone_uuid: cfg.zone_uuid,
            manta_compute_id: cfg.manta_compute_id,
            manta_storage_id: cfg.manta_storage_id
        });

        TIMER = setInterval(HEARTBEAT, INTERVAL);
    });
})();
