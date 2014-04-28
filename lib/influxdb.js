B0;95;c/*
 * Flush stats to InfluxDB (http://influxdb.org/)
 *
 * To enable this backend, include 'statsd-influxdb-backend' in the backends
 * configuration array:
 *
 *   backends: ['statsd-influxdb-backend']
 *
 * The backend will read the configuration options from the following
 * 'influxdb' hash defined in the main statsd config file:
 *
 * influxdb: {
 *   host: '127.0.0.1',   // InfluxDB host. (default 127.0.0.1)
 *   port: 8086,          // InfluxDB port. (default 8086)
 *   database: 'dbname',  // InfluxDB database instance. (required)
 *   username: 'user',    // InfluxDB database username. (required)
 *   password: 'pass',    // InfluxDB database password. (required)
 *   flushInterval: 1000  // Flush interval for the internal buffer. (default 1000)
 * }
 *
 */
var util = require("util");
var querystring = require("querystring");
var http = require("http");

function InfluxdbBackend(startupTime, config, events){
    var self = this;

    self.debug = config.debug;
    self.registry = {};

    self.defaultHost = "127.0.0.1";
    self.defaultPort = 8086;
    self.defaultFlushInterval = 1000;

    self.host = self.defaultHost;
    self.port = self.defaultPort;
    self.proxyInterval = self.defaultProxyInterval;

    if(config.influxdb){
        self.host = config.influxdb.host || self.defaultHost;
        self.port = config.influxdb.port || self.defaultPort;
        self.user = config.influxdb.username;
        self.pass = config.influxdb.password;
        self.database = config.influxdb.database;

        self.flushInterval = config.influxdb.flushInterval || self.defaultFlushInterval;
    }

    self.log("Starting the buffer flush interval. (every " + self.flushInterval + "ms)");
    setInterval(function(){
        self.flushQueue();
    }, self.flushInterval);

    events.on("packet", function(packet, rinfo){
        try {
            self.processPacket(packet, rinfo);
        } catch(e){
            self.log(e);
        }
    });

    return true;
}

InfluxdbBackend.prototype.log = function(msg){
    util.log("[influxdb] " + msg);
};

InfluxdbBackend.prototype.logDebug = function(msg){
    if(this.debug){
        var string;

        if(msg instanceof Function){
            string = msg();
        } else {
            string = msg;
        }

        util.log("[influxdb] (DEBUG) " + string);
    }
};

InfluxdbBackend.prototype.processPacket = function(packet, rinfo){
    var self = this;
    var ts = (new Date()).valueOf();

    /* Stolen from statsd's stats.js. */
    var packet_data = packet.toString();
    var metrics;

    if(packet_data.indexOf("\n") > -1){
        metrics = packet_data.split("\n");
    } else {
        metrics = [packet_data];
    }

    for(var midx in metrics){
        if(metrics[midx].length === 0){
            continue;
        }
        var bits = metrics[midx].toString().split(":");
        var key = bits.shift()
                      .replace(/\s+/g, "_")
                      .replace(/\//g, "-")
                      .replace(/[^a-zA-Z_\-0-9\.]/g, "");

        if(bits.length === 0){
            bits.push("1");
        }

        for(var i = 0; i < bits.length; i++){
            var fields = bits[i].split("|");
            if(fields[1] === undefined){
                self.log('Bad line: ' + fields + ' in msg "' + metrics[midx] +'"');
                continue;
            }

            var tags = {};
            // find any tags on the end of the metric
            // they will look like: "#tag,key=value,some=data
            if(fields[fields.length - 1].indexOf("#") === 0){
                var rawTags = fields[fields.length - 1].substr(1).split(",");
                rawTags.forEach(function(rawTag){
                    var parts = rawTag.split("=");
                    if(parts.length === 1){
                        parts.push(true);
                    }
                    tags[parts[0]] = parts[1];
                });
            }

            var metric_type = fields[1].trim();

            /* Timer */
            if(metric_type === "ms"){
                var value = Number(fields[0] || 0);
                // take into account the sample rate
                if(fields.length > 2 && fields[2].indexOf("@") === 0){
                    value = value / Number(fields[2]);
                }
                self.enqueue("timer", ts, key, value, tags);
            /* Gauge */
            } else if(metric_type === "g"){
                if(fields[0].match(/^[-+]/)){
                    self.logDebug("Sending gauges with +/- is not supported yet.");
                } else {
                    self.enqueue('gauge', ts, key, Number(fields[0] || 0), tags);
                }
            /* Set */
            } else if(metric_type === "s"){
                self.logDebug("Sets not supported yet.");
            /* Counter */
            } else if(metric_type === "c"){
                var value = Number(fields[0] || 1);
                self.enqueue("counter", ts, key, value, tags);
            } else {
                self.logDebug("Unknown metric type: " + metric_type);
            }
        }
    }
};

InfluxdbBackend.prototype.enqueue = function(type, ts, key, value, tags){
    var self = this;

    key = key + "." + type;

    if(!self.registry[key]){
        self.registry[key] = [];
    }

    var data = {value: value, time: ts};
    for(var property in tags){
        if(!data.hasOwnProperty(property)){
            data[property] = tags[property];
        }
    }
    self.registry[key].push(data);
};

InfluxdbBackend.prototype.flushQueue = function(){
    var self = this;
    var registry = self.clearRegistry();
    var points = [];

    for(var key in registry){
        var payload = self.assembleEvent(key, registry[key]);

        self.logDebug(function(){
            return "Flush " + registry[key].length + " values for " + key;
        });

        points.push(payload);
    }

    self.httpPOST(points);

    self.logDebug("Queue flushed");
};


InfluxdbBackend.prototype.clearRegistry = function(){
  var self = this;
  var registry = self.registry;

    self.registry = {};

    return registry;
};

InfluxdbBackend.prototype.assembleEvent = function(name, events){
    var self = this;

    var payload = {
        name: name,
        columns: Object.keys(events[0]),
        points: [],
    };

    for(var idx in events){
        var event = events[idx];
        var points = [];

        for(var cidx in payload.columns){
        var column = payload.columns[cidx];

            points.push(event[column]);
        }

        payload.points.push(points);
    }

    return payload;
};

InfluxdbBackend.prototype.httpPOST = function(points){
  /* Do not send if there are no points. */
    if(!points.length){ return; }

    var self = this;
    var query= {u: self.user, p: self.pass, time_precision: "m"};

    self.logDebug(function(){
        return "Sending " + points.length + " different points via HTTP";
    });

    var options = {
        hostname: self.host,
        port: self.port,
        path: "/db/" + self.database + "/series?" + querystring.stringify(query),
        method: "POST",
    };

    var req = http.request(options);

    req.on("response", function(res){
        var status = res.statusCode;

        if(status !== 200){
            self.log("HTTP Error: " + status);
        }
    });

    req.on("error", function(e, i){
        self.log(e);
    });

    self.logDebug(function(){
        var str = JSON.stringify(points);
        var size = (Buffer.byteLength(str) / 1024).toFixed(2);

        return "Payload size " + size + " KB";
    });

    req.write(JSON.stringify(points));
    req.end();
};

InfluxdbBackend.prototype.configCheck = function(){
    var self = this;
    var success = true;

    /* Make sure the database name and credentials are configured. */
    if(!self.user){
        self.log("Missing config option: username");
        success = false;
    }
    if(!self.pass){
        self.log("Missing config option: password");
        success = false;
    }
    if(!self.database){
        self.log("Missing config option: database");
        success = false;
    }

    return success;
};

exports.init = function(startupTime, config, events){
    var influxdb = new InfluxdbBackend(startupTime, config, events);

    return influxdb.configCheck();
};
