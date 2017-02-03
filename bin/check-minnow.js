/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * check-minnow.js: verifies that the specified host's minnow record is
 * up-to-date.  Exits 0 on success and non-zero otherwise.  This is invoked by
 * amon via the check-minnow bash script in the same directory.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var cmdutil = require('cmdutil');
var dashdash = require('dashdash');
var fs = require('fs');
var moray = require('moray');
var path = require('path');
var VError = require('verror');

var OPTIONS, USAGE_MESSAGE;

OPTIONS = [ {
    'names': [ 'help', 'h' ],
    'type': 'bool',
    'help': 'print this message and exit.'
}, {
    'names': [ 'file', 'f' ],
    'type': 'string',
    'help': 'path to minnow configuration file',
    'default': path.join(__dirname, '..', 'etc', 'config.json')
}, {
    'names': [ 'max-age', 'a' ],
    'type': 'positiveInteger',
    'help': 'maximum age in seconds for a valid record',
    'default': 900
} ];

USAGE_MESSAGE = [
    'Checks the minnow heartbeat record for the current host to make sure ',
    'that it\'s relatively recent.  Returns 0 if the record appears ',
    'up-to-date and non-zero otherwise.'
].join('\n');

/*
 * This data structure is used throughout this program to keep track of the
 * state of the operation.
 */
var checkMinnow = {
    /* bunyan log */
    'cm_log': null,
    /* dash-dash CLI argument parser */
    'cm_parser': null,
    /* raw dashdash-parsed command-line options */
    'cm_args': null,
    /* minnow configuration (file path) */
    'cm_minnow_path': null,
    /* minnow configuration (parsed JSON) */
    'cm_minnow_config': null,
    /* moray client */
    'cm_moray': null
};

function main()
{
    parseArguments(checkMinnow);
    parseMinnowConfig(checkMinnow);
    morayInit(checkMinnow, function (err) {
        if (err) {
            cmdutil.fail(err);
        }

        minnowCheck(checkMinnow, function (err2) {
            if (err2) {
                cmdutil.fail(err2);
            }

            checkMinnow.cm_moray.close();
        });
    });
}

/*
 * Process command-line arguments using dashdash and populate the "checkMinnow"
 * structure (in "cm") with the results.
 */
function parseArguments(cm)
{
    assert.strictEqual(checkMinnow.cm_log, null);

    cm.cm_log = bunyan.createLogger({
        'level': process.env['LOG_LEVEL'] || 'warn',
        'name': path.basename(__filename)
    });

    cm.cm_parser = new dashdash.Parser({ 'options': OPTIONS });

    cmdutil.configure({
        'synopses': [ '[-a | --max-age NSECONDS]' ],
        'usageMessage': USAGE_MESSAGE + '\n\n' +
            cm.cm_parser.help().trimRight()
    });

    try {
        cm.cm_args = cm.cm_parser.parse(process.argv);
        assert.object(cm.cm_args);
    } catch (ex) {
        cmdutil.usage(ex);
    }

    if (cm.cm_args.help) {
        cmdutil.usage();
    }

    if (cm.cm_args._args.length > 0) {
        cmdutil.usage('unexpected arguments');
    }

    cm.cm_minnow_path = cm.cm_args.file;
    cm.cm_maxage = cm.cm_args.max_age;

    cm.cm_log.debug({
        'minnowConfigPath': cm.cm_minnow_path,
        'maxAge': cm.cm_maxage
    }, 'init');
}

/*
 * Read and parse the minnow configuration file specified in "cm" and populate
 * "cm" with the results.
 */
function parseMinnowConfig(cm)
{
    var contents, parsed;

    assert.string(cm.cm_minnow_path);
    try {
        contents = fs.readFileSync(cm.cm_minnow_path, 'utf8');
    } catch (ex) {
        cmdutil.fail(new VError(ex, 'read minnow config "%s"',
            cm.cm_minnow_path));
    }

    try {
        parsed = JSON.parse(contents);
    } catch (ex) {
        cmdutil.fail(new VError(ex, 'parse minnow config "%s"',
            cm.cm_minnow_path));
    }

    if (typeof (parsed) != 'object' || parsed === null ||
        typeof (parsed.moray) != 'object' || parsed.moray === null ||
        typeof (parsed.moray.morayConfig) != 'object' ||
        parsed.moray.morayConfig === null ||
        typeof (parsed.zone_uuid) != 'string' ||
        typeof (parsed.domain) != 'string' ||
        typeof (parsed.moray.bucket) != 'object' ||
        parsed.moray.bucket === null ||
        typeof (parsed.moray.bucket.name) != 'string') {
        cmdutil.fail('validate minnow config "%s": unexpected contents',
            cm.cm_minnow_path);
    }

    if (typeof (parsed.zone_uuid) != 'string') {
        cmdutil.fail('"zone_uuid" missing from minnow configuration');
        return;
    }

    if (typeof (parsed.domain) != 'string') {
        cmdutil.fail('"domain" missing from minnow configuration');
        return;
    }

    cm.cm_minnow_config = parsed;
}

/*
 * Connect to Moray.
 */
function morayInit(cm, callback)
{
    var cfg, morayargs, k;
    var done = false;

    cfg = cm.cm_minnow_config.moray.morayConfig;
    morayargs = {};
    for (k in cfg) {
        morayargs[k] = cfg[k];
    }

    morayargs.log = cm.cm_log.child({ 'component': 'MorayClient' });
    morayargs.mustCloseBeforeNormalProcessExit = true;
    morayargs.failFast = true;
    cm.cm_moray = moray.createClient(morayargs);
    cm.cm_moray.on('error', function (err) {
        assert.ok(!done);
        done = true;
        callback(err);
    });
    cm.cm_moray.on('connect', function () {
        assert.ok(!done);
        done = true;
        callback();
    });
}

/*
 * Given a connected Moray client, check the Minnow record.
 */
function minnowCheck(cm, callback)
{
    var bucket, zonename, domain, key;

    /*
     * The configuration has already been validated at this point.
     */
    bucket = cm.cm_minnow_config.moray.bucket.name;
    assert.string(bucket);
    domain = cm.cm_minnow_config.domain;
    assert.string(domain);
    zonename = cm.cm_minnow_config.zone_uuid;
    assert.string(zonename);
    key = zonename + '.' + domain;
    cm.cm_moray.getObject(bucket, key, function (err, obj) {
        var now, earliest, found;

        if (err) {
            callback(new VError(err, 'getObject "%s"', key));
            return;
        }

        if (typeof (obj.value) != 'object' || obj.value === null ||
            typeof (obj.value.timestamp) != 'number') {
            callback(new VError('getObject "%s": unexpected response', key));
            return;
        }

        now = new Date();
        earliest = new Date(now.getTime() - (cm.cm_maxage * 1000));
        found = new Date(obj.value.timestamp);
        cm.cm_log.debug({
            'now': now.toISOString(),
            'earliest': earliest.toISOString(),
            'found': found.toISOString()
        }, 'timestamp check');

        if (found.getTime() > now.getTime()) {
            /*
             * We're running on the same system that would have captured the
             * timestamp that's in the record.  This really should be impossible
             * unless the clock goes backwards.  If that happens, we may fire a
             * spurious alarm, but it's likely worth notifying somebody about.
             */
            callback(new VError(
                'record "%s" has timestamp from the future: "%s"', key,
                found.toISOString()));
            return;
        }

        if (found.getTime() < earliest.getTime()) {
            callback(new VError(
                'record "%s" is too old (found timestamp "%s", ' +
                'expecting one from at least "%s"', key,
                found.toISOString(), earliest.toISOString()));
            return;
        }

        callback();
    });
}

main();
