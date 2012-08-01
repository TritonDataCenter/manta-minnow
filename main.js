// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var moray = require('moray');
var getopt = require('posix-getopt');
var statvfs = require('statvfs');
var vasnyc = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'minnow',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        stream: process.stdout
});

var TIMER;



///--- Internal Functions

function usage(msg) {
        if (msg)
                console.error(msg);

        var str = 'usage: ' + path.basename(process.argv[1]);
        str += '[-v] [-f file]';
        console.error(str);
        process.exit(1);
}


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


function run(opts) {
        var client = opts.client;
        var key = os.hostname() + '.' + opts.domain;
        var status;


        vasnyc.pipeline({ funcs: [
                function statfs(_, cb) {
                        stat(opts.objectRoot, function (err, s) {
                                if (err)
                                        return (cb(err));

                                s.hostname = key;
                                s.datacenter = opts.datacenter;
                                s.server_uuid = opts.server_uuid;
                                s.zone_uuid = opts.zone_uuid;
                                status = s;
                                return (cb());
                        });
                },

                function updateMoray(_, cb) {
                        LOG.debug({
                                bucket: opts.bucket,
                                key: key,
                                data: status
                        }, 'Writing current status');
                        client.putObject(opts.bucket, key, status, cb);
                }
        ] }, function (err) {
                if (err) {
                        LOG.error({
                                bucket: opts.bucket,
                                key: key,
                                status: status,
                                err: err
                        }, 'Failed to update status');
                } else {
                        LOG.info({
                                bucket: opts.bucket,
                                key: key,
                                status: status
                        }, 'status updated');
                }
        });
}


function setupAndRun(cfg) {
        var bucket = cfg.moray.bucket.name;
        var client = moray.createClient({
                connectTimeout: cfg.moray.connectTimeout,
                log: LOG,
                host: cfg.moray.host,
                port: cfg.moray.port
        });
        var index = {
                index: cfg.moray.bucket.index
        };
        var timer;

        client.once('connect', function () {
                LOG.info('morayClient: connected');
                // Guarantee the bucket exists, then just schedule the runner
                client.putBucket(bucket, index, function (err) {
                        if (err) {
                                LOG.fatal(err, 'unable to putBucket');
                                process.exit(1);
                        }

                        LOG.info({
                                bucket: bucket,
                                objectRoot: cfg.objectRoot
                        }, 'Moray bucket Ok. Starting stat daemon');
                        timer = setInterval(function heartbeat() {
                                var opts = {
                                        bucket: bucket,
                                        client: client,
                                        datacenter: cfg.datacenter,
                                        domain: cfg.domain,
                                        objectRoot: cfg.objectRoot,
                                        server_uuid: cfg.server_uuid,
                                        zone_uuid: cfg.zone_uuid
                                };
                                run(opts);
                        }, (cfg.interval || 5000));
                });

                client.removeAllListeners('error');

                client.once('close', function (had_err) {
                        LOG.warn('moray client closed, reestablishing...');
                        clearInterval(timer);
                        client.removeAllListeners('error');
                        client = null;
                        setupAndRun(cfg);
                });

                client.once('error', function (err) {
                        LOG.error(err, 'morayClient: underlying error');
                        // Do nothing- close will fire next
                });
        });

        client.once('error', function (err) {
                LOG.fatal(err, 'morayClient: unable to connect; retrying');
                client.removeAllListeners('connect');
                setupAndRun(cfg);
        });
}



///--- Mainline

setupAndRun(readConfig(parseOptions().file));
