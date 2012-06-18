// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert');
var fs = require('fs');
var os = require('os');

var bunyan = require('bunyan');
var moray = require('moray-client');
var optimist = require('optimist');
var statvfs = require('statvfs');
var vasnyc = require('vasync');



///--- Globals

var ARGV = optimist.options({
        'd': {
                alias: 'debug',
                describe: 'debug level'
        },
        'f': {
                alias: 'file',
                describe: 'configuration file',
                demand: true
        }
}).argv;

var LOG = bunyan.createLogger({
        level: ARGV.d ? (ARGV.d > 1 ? 'trace' : 'debug') : 'info',
        name: 'minnow',
        serializers: {
                err: bunyan.stdSerializers.err
        },
        src: ARGV.d ? true : false,
        stream: process.stdout
});



///--- Internal Functions

function errorAndExit(err, msg) {
        LOG.fatal({err: err}, msg);
        process.exit(1);
}


function readConfig() {
        var cfg;
        var file;

        try {
                file = fs.readFileSync(ARGV.f, 'utf8');
        } catch (e) {
                errorAndExit(e, 'unable to read %s', ARGV.f);
        }

        try {
                cfg = JSON.parse(file);
        } catch (e) {
                errorAndExit(e, 'invalid JSON in %s', ARGV.f);
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
                        client.put(opts.bucket, key, status, cb);
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


///--- Mainline

// Because everyone asks, the vars here are prefixed with '_' to not consume
// the global namespace, and then cause scope errors from javascriptlint
// in functions.

var _cfg = readConfig();
var _bucket = _cfg.moray.bucket.name;
var _client = moray.createClient({
        connectTimeout: _cfg.moray.connectTimeout,
        log: LOG,
        url: _cfg.moray.url
});
var _schema = {
        schema: _cfg.moray.bucket.schema
};
// Guarantee the bucket exists, then just schedule the runner
_client.putBucket(_bucket, _schema, function (err) {
        if (err)
                errorAndExit(err, 'unable to putBucket');

        LOG.info({
                bucket: _bucket,
                objectRoot: _cfg.objectRoot
        }, 'Moray bucket Ok. Starting stat daemon');
        setInterval(function heartbeat() {
                var opts = {
                        bucket: _bucket,
                        client: _client,
                        datacenter: _cfg.datacenter,
                        domain: _cfg.domain,
                        objectRoot: _cfg.objectRoot
                };
                run(opts);
        }, (_cfg.interval || 5000));
});
