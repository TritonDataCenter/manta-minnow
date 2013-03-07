// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var fs = require('fs');
var os = require('os');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var moray = require('moray');
var getopt = require('posix-getopt');
var once = require('once');
var statvfs = require('statvfs');



///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'minnow',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        stream: process.stdout
});
var HEARTBEAT;
var INTERVAL;
var TIMER;



///--- CLI Functions

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('vf:(file)', process.argv);

        while ((option = parser.getopt()) !== undefined) {
                switch (option.option) {
                case 'f':
                        opts.file = option.optarg;
                        break;

                case 'v':
                        // Allows us to set -vvv -> this little hackery
                        // just ensures that we're never < TRACE
                        LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                        if (LOG.level() <= bunyan.DEBUG)
                                LOG = LOG.child({src: true});
                        break;

                default:
                        usage();
                        break;
                }
        }

        return (opts);
}


function readConfig(fname) {
        var cfg;
        var file;

        try {
                file = fs.readFileSync(fname, 'utf8');
        } catch (e) {
                LOG.fatal(e, 'unable to read %s', fname);
                process.exit(1);
        }
        try {
                cfg = JSON.parse(file);
        } catch (e) {
                LOG.fatal(e, 'invalid JSON in %s', fname);
                process.exit(1);
        }

        return (cfg);
}


function usage(msg) {
        if (msg)
                console.error(msg);

        var str = 'usage: ' + path.basename(process.argv[1]);
        str += '[-v] [-f file]';
        console.error(str);
        process.exit(1);
}



///--- worker functions


function createMorayClient(opts, cb) {
        assert.object(opts, 'options');
        assert.optionalObject(opts.retry, 'options.retry');
        assert.func(cb, 'callback');

        cb = once(cb);

        var retry = opts.retry || {};
        var client = moray.createClient({
                connectTimeout: opts.connectTimeout,
                log: LOG,
                host: opts.host,
                port: opts.port,
                retry: (opts.retry === false ? false : {
                        retries: retry.retries || Infinity,
                        minTimeout: retry.minTimeout || 1000,
                        maxTimeout: retry.maxTimeout || 60000
                })
        });

        function setup() {
                var bname = opts.bucket.name;
                var index = opts.bucket.index;

                client.putBucket(bname, {index: index}, function (err) {
                        if (err) {
                                LOG.error(err, 'moray: putBucket: failed');
                                setTimeout(function () {
                                        setup();
                                }, 2000);
                                return;
                        }

                        cb(null, client);
                });
        }

        function onConnect() {
                client.removeListener('error', onError);
                LOG.info({moray: client.toString()}, 'moray: connected');

                client.on('close', function () {
                        LOG.error('moray: closed: stopping heartbeats');
                        clearInterval(TIMER);
                });

                client.on('connect', function () {
                        LOG.info('moray: connect: starting heartbeats');
                        TIMER = setInterval(HEARTBEAT, INTERVAL);
                });

                client.on('error', function (err) {
                        LOG.warn(err, 'moray: error (reconnecting)');
                });

                setup();
        }

        function onError(err) {
                client.removeListener('connect', onConnect);
                LOG.error(err, 'moray: connection failed');
                setTimeout(createMorayClient.bind(null, opts, cb), 1000);
        }

        function onConnectAttempt(number, delay) {
                var level;
                if (number === 0) {
                        level = 'info';
                } else if (number < 5) {
                        level = 'warn';
                } else {
                        level = 'error';
                }
                LOG[level]({
                        attempt: number,
                        delay: delay
                }, 'moray: connection attempted');
        }

        client.once('connect', onConnect);
        client.once('error', onError);
        client.on('connectAttempt', onConnectAttempt); // this we always use
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

var _opts = parseOptions();
var _cfg = readConfig(_opts.file);

createMorayClient(_cfg.moray, function (err, client) {
        assert.ifError(err); // should never return error

        LOG.info({
                bucket: _cfg.moray.bucket,
                objectRoot: _cfg.objectRoot
        }, 'moray setup done: starting stat daemon');

        // Set up globals so we can disable/reenable in the moray
        // connection status handlers
        INTERVAL = _cfg.interval || 30000;
        HEARTBEAT = heartbeat.bind(null, {
                bucket: _cfg.moray.bucket.name,
                datacenter: _cfg.datacenter,
                domain: _cfg.domain,
                moray: client,
                objectRoot: _cfg.objectRoot,
                server_uuid: _cfg.server_uuid,
                zone_uuid: _cfg.zone_uuid,
                manta_compute_id: _cfg.manta_compute_id,
                manta_storage_id: _cfg.manta_storage_id
        });
        TIMER = setInterval(HEARTBEAT, INTERVAL);
});
