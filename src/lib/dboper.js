/**
 * 该文件用来初始化数据库的连接
 */
var redis    = require('ioredis');
var mongoose = require('mongoose');
var Schema   = require('mongoose').Schema;

function DbOper() {
    this.redisClient = null;
    if(global._redisClient) {
        this.redisClient = global._redisClient;
    }
    this.mongo  = null;
    this.Schema = mongoose.Schema;

    this.setRedisPara = function(para) {
        redisPara.port = process.env.LVZHOUV3_CONFIG_REDIS_PORT || para.port;
        redisPara.host = process.env.LVZHOUV3_CONFIG_REDIS_HOST || para.host;
    };
    this.getRedisPara = function() {
        return redisPara;
    };
    this.setMongoPara = function(para) {
        mongoPara = para;
    };
    this.getMongoPara = function(bIsMonitor) {
        var res = mongoPara.indexOf('@');
        var arr1;
        var headstr = "";

        //判断是否有用户名和密码
        if (res > 0) {
            arr1 = mongoPara.split('@');
            headstr = arr1[0] + '@';
        } else {
            arr1 = mongoPara.split('//');
            headstr = arr1[0] + '//';
        }
        var arr2 = arr1[1].split('/');

        //判断是否包含端口号，
        res = arr2[0].indexOf(':');
        if (res < 0) {
            console.warn("Invalid mongodb para: " + mongoPara);
            return mongoPara;
        }
        var arr3 = arr2[0].split(':');

        if (bIsMonitor && (true == bIsMonitor)) {
          var mongoHost = process.env.LVZHOUV3_CONFIG_MONITOR_MONGODB_HOST || arr3[0];
          var mongoPort = process.env.LVZHOUV3_CONFIG_MONITOR_MONGODB_PORT || arr3[1];
        } else {
          var mongoHost = process.env.LVZHOUV3_CONFIG_MONGODB_HOST || arr3[0];
          var mongoPort = process.env.LVZHOUV3_CONFIG_MONGODB_PORT || arr3[1];
        }

        //结合环境变量实际情况重新组装连接mongodb的参数
        mongoPara = headstr + mongoHost + ':' + mongoPort +  '/' + arr2[1];
        return mongoPara;
    };

    // 私有属性
    // redis数据库连接参数，形如：{'port':6379, 'host':'192.168.110.34'}。默认为微软云上的环境
    var redisPara = {'port':6379, 'host':'h3crd-wlan39'};
    // mongoose数据库连接参数，形如：'mongodb://admin:admin@192.168.110.33:40000/lyytest'。默认为微软云上的环境
    var mongoPara = 'mongodb://admin:admin@h3crd-wlan16:27017/WLAN';
    this.setRedisPara(redisPara);
}

// 连接redis数据库，如果只需要连接redis数据库，可调用此函数
// redisPara形如：{'port':6379, 'host':'192.168.110.34'}
DbOper.prototype.connectRedis = function(redisPara) {
    if(global._redisClient) {
        this.redisClient = global._redisClient;
        return;
    }
    if (redisPara != undefined) {
        this.setRedisPara(redisPara);
    }
    var para = this.getRedisPara();

    if (process.env.LVZHOUV3_CONFIG_REDIS_SINGLE_MODE) {
        this.redisClient = global._redisClient = new redis(para);
    } else {
        global._redisClient = this.redisClient = new redis.Cluster([{
            port: para.port,
            host: para.host
        }]);
    }

    this.redisClient.on("connect", function(){
        console.warn((new Date()) + ' Connect to redis success: ' + JSON.stringify(para));
    });
    this.redisClient.on("end", function(){
        console.warn((new Date()) + ' End connection to redis.');
    });
    this.redisClient.on("error", function(err){
        console.error((new Date()) + " redis Error: " + err + ", redis option: " + JSON.stringify(para));
    });
};

DbOper.prototype.connectRedisFromConfig = function (redisPara, myoptions) {

    if (arguments.length == 0 || typeof redisPara != "object") {
        throw new Error('redis init param error:' + arguments)
    }
    var options=myoptions||{};
    if (process.env.LVZHOUV3_CONFIG_REDIS_SINGLE_MODE) {
        redisPara.keepAlive = options.keepAlive || 1000;
        redisPara.connectTimeout = options.connectTimeout || 5000;
        var redisClient = new redis(redisPara);
    } else {
        var cluster_option = {};
        cluster_option.clusterRetryStrategy = options.clusterRetryStrategy || function (times) {
                var delay = Math.min(100 + times * 2, 2000);
                return delay;
            };
        cluster_option.redisOptions = {
            keepAlive: options.keepAlive || 1000,
            connectTimeout: options.connectTimeout || 5000
        }
        var redisClient = new redis.Cluster([{
            port: redisPara.port,
            host: redisPara.host
        }], cluster_option);
    }


    redisClient.on("connect", function () {
        console.warn(' Connect to redis success: ' + JSON.stringify(redisPara));
    });
    redisClient.on("end", function () {
        console.warn(' End connection to redis.');
    });
    redisClient.on("error", function (err) {
        console.error(" redis Error: " + err + ", redis option: " + JSON.stringify(redisPara));
    });
    return redisClient;
};

// 连接mongoose数据库，如果只需要连接mongoose数据库，可调用此函数
// mongoosePara形如：'mongodb://admin:admin@192.168.110.33:40000/lyytest'
DbOper.prototype.connectMongoose = function(mongoosePara, bIsMonitor, poolSize) {
    if ((mongoosePara != undefined) && ('string' == typeof mongoosePara)) {
        this.setMongoPara(mongoosePara);
    } else {
        bIsMonitor = mongoosePara;
    }
    var argLen = arguments.length;
    switch(argLen) {
        case 2: {
            if ('boolean' == typeof bIsMonitor) {
                poolSize = 5;
            } else {
                poolSize   = bIsMonitor;
                bIsMonitor = false;
            }
            break;
        }
    }

    if (300 < poolSize || 1 > poolSize) {
        console.warn('The size %d of the pool you set is improper,setting to default now!', poolSize);
        poolSize = 5;
    }
    if (undefined == poolSize || 'boolean' == typeof poolSize) {
        poolSize = 5;
    }

    var para = this.getMongoPara(bIsMonitor);
    var options = {
        'server': {
            'reconnectTries': Number.MAX_VALUE,
            'reconnectInterval': 5000,
            'poolSize': poolSize
        },
        'db': {'readPreference': 'secondaryPreferred'}
    };
    mongoose.Promise = global.Promise;
    this.mongo = mongoose.createConnection(para, options);
    var keepaliveSchema = new Schema({
        name: String
    });

    //为mongodb保活而定义的一张空表
    var keepaliveModel = this.mongo.model("mongo_keepalive", keepaliveSchema);
    var timerHandle = null;
    var serName = para.split('/').slice(3);
    var iSearchNoReturn = 0;
    var imongopara = mongoosePara;
    //定时器超时调用该函数查找一次空表，达到保活目的
    function procDbKeepalive() {

        if (iSearchNoReturn >= 5) {
            iSearchNoReturn = 0;
            console.error('mongodb connection is wrong, process exit!!!!!! ', imongopara);
            //setTimeout(function(){
            //    process.exit(7);
            // }, 2000);
            return;
        }

        iSearchNoReturn++;

        keepaliveModel.findOne({name: "mongo_keepalive"}, function (err, result) {

            iSearchNoReturn = 0;

            if (err) {
                console.error("find mongo_keepalive with error: " + err);
            } else {
                console.log(serName + ' find mongo_keepalive from mongoose success.');
            }
        });
    }

    this.mongo.on('connecting', function () {
        console.warn((new Date()) + ' connecting to mongoose %s.', para);
    });

    this.mongo.on('open', function () {
        console.warn((new Date()) + ' open to mongoose %s.', para);
    });

    this.mongo.on('connected', function () {
        console.warn((new Date()) + ' connected to mongoose success: ' + para + ', poolSize: ' + poolSize);
        if (null == timerHandle) {
            timerHandle = setInterval(procDbKeepalive, 30000);
        }
    });

    this.mongo.on('disconnected', function () {
        console.warn((new Date()) + ' disconnected to mongoose %s.', para);
        if (null != timerHandle) {
            clearTimeout(timerHandle);
            timerHandle = null;
        }
    });

    this.mongo.on('close', function () {
        console.warn((new Date()) + ' close to mongoose %s.', para);
    });

    function connectFailed() {
        process.exit(7);
    }

    this.mongo.on('error', function (error) {
        console.error((new Date()) + ' Connect to mongoose with error: ' + error + ', para: ' + para);
        setTimeout(connectFailed, 30000);
    });
};

DbOper.prototype.connectMongooseFromConfig = function (mongoosePara, bIsMonitor, poolSize) {
    if (!mongoosePara || 'string' != (typeof mongoosePara)) {
        throw new Error('mongooose param error: %s', mongoosePara)
    }
    var argLen = arguments.length;
    switch (argLen) {
        case 2: {
            if ('boolean' == typeof bIsMonitor) {
                poolSize = 5;
            } else {
                poolSize = bIsMonitor;
                bIsMonitor = false;
            }
            break;
        }
    }

    if (300 < poolSize || 1 > poolSize) {
        console.warn('The size %d of the pool you set is improper,setting to default now!', poolSize);
        poolSize = 5;
    }
    if (undefined == poolSize || 'boolean' == typeof poolSize) {
        poolSize = 5;
    }

    var para = mongoosePara;
    var options = {
        'server': {
            'reconnectTries': Number.MAX_VALUE,
            'reconnectInterval': 5000,
            'poolSize': poolSize,
            "socketOptions": {
				"keepAlive": 30000,
				"connectTimeoutMS": 30000
			  }
        },
        'db': {'readPreference': 'secondaryPreferred'}
    };
	mongoose.Promise = global.Promise;
    this.mongo = mongoose.createConnection(para, options);
    var keepaliveSchema = new Schema({
        name: String
    });

    //为mongodb保活而定义的一张空表
    var keepaliveModel = this.mongo.model("mongo_keepalive", keepaliveSchema);
    var timerHandle = null;
    var serName = para.split('/').slice(3);
    var iSearchNoReturn = 0;
    var imongopara = mongoosePara;
    //定时器超时调用该函数查找一次空表，达到保活目的
    function procDbKeepalive() {

        if (iSearchNoReturn >= 5)
        {
            console.error('mongodb connection is wrong, process exit!!!!!! ', imongopara);
            //setTimeout(function(){
            //    process.exit(7);
            // }, 2000);
            return;
        }

        iSearchNoReturn++;

        keepaliveModel.findOne({name: "mongo_keepalive"}, function(err, result) {

            iSearchNoReturn = 0;

            if (err) {
                console.error("find mongo_keepalive with error: " + err);
            }else {
                console.log(serName + ' find mongo_keepalive from mongoose success.');
            }
        });
    }

    this.mongo.on('connecting', function() {
        console.warn((new Date()) + ' connecting to mongoose %s.', para);});

    this.mongo.on('open', function() {
        console.warn((new Date()) + ' open to mongoose %s.', para);});

    this.mongo.on('connected', function() {
        console.warn((new Date()) + ' connected to mongoose success: ' + para + ', poolSize: ' + poolSize);
        if (null == timerHandle) {
            timerHandle = setInterval(procDbKeepalive, 30000);
        }
    });

    this.mongo.on('disconnected', function() {
        console.warn((new Date()) + ' disconnected to mongoose %s.', para);
        if (null != timerHandle) {
            clearTimeout(timerHandle);
            timerHandle = null;
        }
    });

    this.mongo.on('close', function() {
        console.warn((new Date()) + ' close to mongoose %s.', para);
    });

    function connectFailed() {
        process.exit(7);
    }

    this.mongo.on('error', function(error) {
        console.error((new Date()) + ' Connect to mongoose with error: ' + error + ', para: ' + para);
        setTimeout(connectFailed, 30000);
    });
};

// 连接redis数据库和mongoose数据库
DbOper.prototype.connectDatabase = function(redisPara, mongoosePara, mongoPoolSize) {
    if(redisPara)
    {
        this.connectRedis(redisPara);
    }
    if(mongoosePara)
    {
        this.connectMongoose(mongoosePara, mongoPoolSize);
    }
};

var dboper = module.exports = new DbOper;