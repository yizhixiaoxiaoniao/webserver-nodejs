/**
 * Created by Juneja on 2015/9/7.
 *
 * Description: Common methods for RabbitMQ(github.com/postwait/node-amqp)
 */
"use strict";

var amqp    = require('amqp'),
    cons    = require('./constants'),
    basic   = require('./basic');

var broadcast_key = "broadcast_key";
var implOptions = {
    reconnect: false
};

function MqOper() {
    this.connection  = null;
    this.exdirect = null;
    this.exreply  = null;
    this.broadcastReq = null;
    this.exmultiReq   = null;
    this.location = '';   // name,ipve,pid组成

    // V3 to V2
    this.connectionV2  = null;
    this.exchgV2List   = {};

    /*
     * key: an MQ publish option field
     * value: optional parameter, if doesn't provide then return key's value
     * description: set/get publish option for webserver to MQ
     * */
    this.setSendOption = function (key, value) {
        switch (arguments.length) {
            case 0:
                return sendOption;
                break;
            case 1:
                return sendOption[key];
                break;
            case 2:
                sendOption[key] = value;
                break;
            default:
                console.error("[MQHD]Too much parameters for set/get SendOption.");
        }
    };
    this.getSendOption = this.setSendOption;

    /*
     * hostArray: MQ server hostnames array, optional parameter
     * description: set/get hostnames of MQ server
     *              if doesn't provide 'hostArray' then return current MQServer hostnames
     * */
    this.setHostnames = function (hostArray) {
        switch (arguments.length) {
            case 0:
                return hostnames;
                break;
            case 1:
                var nodeEnv = process.env.NODE_ENV;
                if (nodeEnv == "production") {
                    hostnames = cons.production_mqhost;
                } else {
                    hostnames = hostArray;
                }
                break;
            default:
                console.error("[MQHD]Too much parameters for set/get Hostnames.");
        }
    };
    this.getHostnames = this.setHostnames;

    /* private, only for reference */
    var hostnames  = ["h3crd-wlan1", "h3crd-wlan2"];
    var sendOption = {
        "replyTo"    : "",
        "appId"      : ""
    };
    var requestMsg = {
        "headers" : "",
        "url"     : "",
        "method"  : "",
        "body"    : ""
    };

    /*
     * key: an request msg field for MQ
     * value: optional parameter, if doesn't provide then return key's value
     * description: set/get request msg for webserver to MQ
     * */
    var _setRequestMsg = function (key, value) {
        switch (arguments.length) {
            case 0:
                return requestMsg;
                break;
            case 1:
                return requestMsg[key];
                break;
            case 2:
                requestMsg[key] = value;
                break;
            default:
                console.error("[MQHD]Too much parameters for set/get RequestMsg.");
        }
    };
    var _getRequestMsg = _setRequestMsg;
}

/*
 * Generate connect option with serviceName
 * */
MqOper.prototype._genConnectOption = function (serviceName) {
    var hosts    = this.getHostnames();
    var num      = hosts.length;
    var hostIdx  = 0;//cons.serviceIdx[serviceName] % num
    var hostname = process.env.LVZHOUV3_CONFIG_MQHOST || hosts[hostIdx] || "172.27.8.115";

    var conOption = {};
    conOption.host  = hostname;
    conOption.login = serviceName;
    conOption.password = "123456";

    return conOption;
};

/*
 * pkey: pblish key (service name)
 * msg: msg to send
 * callback: callback参数可选，如果使用callback参数，则对方必须回复该消息
 * Description: 如果是webserver通过MQ发送http消息，则必须使用callback参数;
 * msg format: JSON格式的字符串
 * */
MqOper.prototype.sendMsg = function (pkey, msg, callback) {
    var sendExchage;
    var option = this.getSendOption();

    delete option["correlationId"];
    if (3 == arguments.length) {
        var reqID = basic.setCbMap(callback);
        option.messageId = reqID;
    }

    if (typeof pkey != 'string') {
        console.error('typeof pkey is not string when send mq message.');
        console.error(pkey);
        return;
    }

    // 发往微服务的mq消息都使用direct exchange
    if (pkey.slice(0, 12) != 'iotwebserver' && pkey.slice(0, 9) != 'webserver') {
        sendExchage = mqoper.exdirect;
    }
    // 发往webserver的mq消息都使用reply exchange
    else {
        sendExchage = mqoper.exreply;
    }

    if(sendExchage) {
        try {
            sendExchage.publish(pkey, msg, option);
            console.log('[MQHD]Send message to %s success by %s.', pkey, sendExchage.name);
            console.log('  message: %s', msg);
            console.log('  key/option = %s/%s.', pkey, JSON.stringify(option));
        } catch(err) {
            console.error((new Date()) + ' [MQHD]Send message to %s failed with error: %s', pkey, err);
        }
    } else {
        console.warn("[MQHD]Exchange is null or undefined!");
    }
};

/*
 * msg: msg to reply
 * deliverInfo: just as subscribed msg's deliverInfo
 * Description: 该接口仅适用于向webserver回复消息
 * */
MqOper.prototype.replyMsg = function (msg, deliveryInfo) {
    var option = this.getSendOption();
    var replyExchage = mqoper.exreply;

    if(replyExchage)
    {
        var pubOption = {};
        pubOption.appId = option.appId;
        if (deliveryInfo.messageId != undefined) {
            pubOption.messageId = deliveryInfo.messageId;
        }

        try {
            replyExchage.publish(deliveryInfo.replyTo, msg, pubOption);
            console.log('[MQHD]Reply message to %s success by %s.', deliveryInfo.replyTo, replyExchage.name);
            console.log('  message: %s', msg);
            console.log('  key/option = %s/%s.', deliveryInfo.replyTo, JSON.stringify(pubOption));
        }
        catch(err) {
            console.error((new Date()) + ' [MQHD]Reply message to %s failed with error: %s', deliveryInfo.replyTo, err);
        }
    } else {
        console.error("[MQHD]Exchange "+replyExchage.name+" not around now!");
    }
};

/*
 * msg: msg to reply
 * deliverInfo: just as subscribed msg's deliverInfo
 * Description: 该接口仅适用于向websocket server回复消息
 * */
MqOper.prototype.replyMsg2WSServer = function (msg, deliveryInfo) {
    var option = this.getSendOption();
    var replyExchage = mqoper.exdirect;

    if(replyExchage)
    {
        var pubOption = {};
        pubOption.appId = option.appId;
        if (deliveryInfo.messageId != undefined) {
            pubOption.messageId = deliveryInfo.messageId;
        }

        try {
            replyExchage.publish(deliveryInfo.replyTo, msg, pubOption);
            console.log('[MQHD]Reply message to %s success by %s.', deliveryInfo.replyTo, replyExchage.name);
            console.log('  message: %s', msg);
            console.log('  key/option = %s/%s.', deliveryInfo.replyTo, JSON.stringify(pubOption));
        }
        catch(err) {
            console.error((new Date()) + ' [MQHD]Reply message to %s failed with error: %s', deliveryInfo.replyTo, err);
        }
    } else {
        console.error("[MQHD]Exchange "+replyExchage.name+" not around now!");
    }
};

MqOper.prototype.broadcastSendMsg = function (message) {
    var broadcastSendExchage;
    var option = JSON.parse(JSON.stringify(this.getSendOption()));

    delete option["correlationId"];   // 避免sendMsg填的字段干扰

    broadcastSendExchage = mqoper.broadcastReq;
    if(broadcastSendExchage)
    {
        option.type = "broadcast";
        try {
            broadcastSendExchage.publish(broadcast_key, message, option);
            console.log('[MQHD]Send message to %s success by %s.', broadcast_key, broadcastSendExchage.name);
            console.log('  message: %s', message);
        }
        catch(err) {
            console.error((new Date()) + ' [MQHD]Send message to %s failed with error: %s', broadcast_key, err);
        }
    } else {
        console.warn("[MQHD]Exchange is null or undefined!");
    }
};

function procMqKeepalive() {
    var key = "mq_keepalive";
    var msg = "keep alive.";
    mqoper.sendMsg(key, msg);
}

/*
 * 函数功能：微服务用来连接MQ服务器的函数
 * 参数说明：1）serviceName： 微服务名
 *         2）recvCallback: 接收MQ消息的回调处理函数
 */
MqOper.prototype.connectReadyForService = function (serviceName, recvCallback) {
    var mqOption = this._genConnectOption(serviceName);

    // set 'replyTo' for reply queue name
    this.setSendOption('appId', serviceName);
    // this.setSendOption('replyTo', serviceName);
    this.setSendOption('replyTo', [serviceName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

    var timerHandle = null;
    var connect = amqp.createConnection(mqOption, implOptions);
    connect.on('ready', function() {
        console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serviceName, JSON.stringify(mqOption));

        mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);   /* 声明 */
        mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);
        mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

        connect.queue(serviceName, cons.queueParas.qOption, function (q1) {
            console.warn("[MQHD]Queue "+serviceName+" for request is open.");
            q1.bind(cons.exchangeParas.exRequest, serviceName);
            q1.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
            q1.subscribe(function(message, header, deliveryInfo) {
                recvCallback(message, header, deliveryInfo);
            });
        });
        if (null == timerHandle) {
            timerHandle = setInterval(procMqKeepalive, 120000);
        }
    });

    connect.on('error', function (err) {
        console.error("[MQHD]Failed to connect MQ for "+serviceName+' with '+err+', mq option: '+JSON.stringify(mqOption));
    });
    connect.on('end', function (err) {
        console.warn("[MQHD]received end event.");
    });
    connect.on('close', function (err) {
        console.warn("[MQHD]received close event.");
        setTimeout(function() {
            connect.reconnect();
        }, 5000);
    });

    return this.connection = connect;
};

/*
 * 函数功能：创建需要ack的mq队列
 * 参数说明：1）serviceName       微服务名
 *         2)qOption为JSON格式的可选参数,目前仅用来设置prefetchCount、x-max-length和x-max-length-bytes，其中：
 *           prefetchCount      可选，用来指定消费者预取的消息个数，默认值为100
 *           x-max-length       可选，用来指定MQ队列最大支持的消息个数，默认值为100000
 *           x-max-length-bytes 可选，用来指定MQ队列最大支持的消息大小，以字节为单位,默认值为10000000
 *         3）recvCallback      回调处理函数
 * 注意事项：该接口为具有ack机制的接口，recvCallback的第四个参数ack必须得到调用，
 *         必须显式的调用mqhd.ack(ack)以完成mq消息的确认，否则未响应的消息数达到prefetchCount后无法再收到任何mq消息
 */
MqOper.prototype.connectReadyForServiceNeedAck = function (serviceName, qOption, recvCallback) {
    if (2 == arguments.length) {
        recvCallback = arguments[1];
    }
    var myPrefetchCount = cons.defaultMQPrefetchCount;
    if (qOption && qOption.prefetchCount && qOption.prefetchCount > 0 && qOption.prefetchCount <= 2*myPrefetchCount) {
        myPrefetchCount = qOption.prefetchCount;
    }
    var myQOption = cons.queueParas.qOptionAck;
    if (qOption && qOption["x-max-length"] && qOption["x-max-length"] > 0 && qOption["x-max-length"] <= 2*myQOption.arguments["x-max-length"]) {
        myQOption.arguments["x-max-length"] = qOption["x-max-length"];
    }
    if (qOption && qOption["x-max-length-bytes"] && qOption["x-max-length-bytes"] > 0 && qOption["x-max-length-bytes"] <= 2*myQOption.arguments["x-max-length-bytes"]) {
        myQOption.arguments["x-max-length-bytes"] = qOption["x-max-length-bytes"];
    }
    console.warn('myQOption: ', JSON.stringify(myQOption));

    var mqOption = this._genConnectOption(serviceName);

    // set 'replyTo' for reply queue name
    this.setSendOption('appId', serviceName);
    // this.setSendOption('replyTo', serviceName);
    this.setSendOption('replyTo', [serviceName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

    var timerHandle = null;
    var connect = amqp.createConnection(mqOption, implOptions);
    connect.on('ready', function() {
        console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serviceName, JSON.stringify(mqOption));

        mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);   /* 声明 */
        mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);
        mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

        connect.queue(serviceName, myQOption, function (q1) {
            console.warn("[MQHD]Queue %s for request is open, prefetchCount: %s, option: %s.", serviceName, myPrefetchCount, JSON.stringify(myQOption));
            q1.bind(cons.exchangeParas.exRequest, serviceName);
            q1.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
            q1.subscribe({ack: true, prefetchCount: myPrefetchCount}, function(message, header, deliveryInfo, ack) {
                recvCallback(message, header, deliveryInfo, ack);
            });
        });
        if (null == timerHandle) {
            timerHandle = setInterval(procMqKeepalive, 120000);
        }
    });

    connect.on('error', function (err) {
        console.error("[MQHD]Failed to connect MQ for "+serviceName+' with '+err+', mq option: '+JSON.stringify(mqOption));
    });
    connect.on('end', function (err) {
        console.warn("[MQHD]received end event.");
    });
    connect.on('close', function (err) {
        console.warn("[MQHD]received close event.");
        setTimeout(function() {
            connect.reconnect();
        }, 5000);
    });

    return this.connection = connect;
};

/*
 * 函数功能：创建需要ack的mq队列(wsserver专用)
 * 参数说明：1)qOption为JSON格式的可选参数,目前仅用来设置prefetchCount、x-max-length和x-max-length-bytes，其中：
 *           prefetchCount      可选，用来指定消费者预取的消息个数，默认值为100
 *           x-max-length       可选，用来指定MQ队列最大支持的消息个数，默认值为100000
 *           x-max-length-bytes 可选，用来指定MQ队列最大支持的消息大小，以字节为单位,默认值为10000000
 *         3）recvCallback      回调处理函数
 * 注意事项：该接口为具有ack机制的接口，如果是非广播队列，recvCallback的第四个参数ack必须得到调用，
 *         必须显式的调用mqhd.ack(ack)以完成mq消息的确认，否则未响应的消息数达到prefetchCount后无法再收到任何mq消息
 */
MqOper.prototype.connectReadyForWSServerNeedAck = function (qOption, recvCallback) {
    var serviceName = 'iotwsserver';

    if (1 == arguments.length) {
        recvCallback = arguments[0];
    }
    var myPrefetchCount = cons.defaultMQPrefetchCount;
    if (qOption && qOption.prefetchCount && qOption.prefetchCount > 0 && qOption.prefetchCount <= 2*myPrefetchCount) {
        myPrefetchCount = qOption.prefetchCount;
    }
    var myQOption = cons.queueParas.qOptionAck;
    if (qOption && qOption["x-max-length"] && qOption["x-max-length"] > 0 && qOption["x-max-length"] <= 2*myQOption.arguments["x-max-length"]) {
        myQOption.arguments["x-max-length"] = qOption["x-max-length"];
    }
    if (qOption && qOption["x-max-length-bytes"] && qOption["x-max-length-bytes"] > 0 && qOption["x-max-length-bytes"] <= 2*myQOption.arguments["x-max-length-bytes"]) {
        myQOption.arguments["x-max-length-bytes"] = qOption["x-max-length-bytes"];
    }
    console.warn('myQOption: ', JSON.stringify(myQOption));

    var mqOption = this._genConnectOption(serviceName);

    // set 'replyTo' for reply queue name
    this.setSendOption('appId', serviceName);
    this.setSendOption('replyTo', [serviceName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

    var timerHandle = null;
    var connect = amqp.createConnection(mqOption, implOptions);
    connect.on('ready', function() {
        console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serviceName, JSON.stringify(mqOption));

        mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);   /* 声明 */
        mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);
        mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

        var pOption = mqoper.getSendOption();
        connect.queue(pOption.replyTo, myQOption, function (q1) {
            console.warn("[MQHD]Queue %s for request is open, prefetchCount: %s, option: %s.", pOption.replyTo, myPrefetchCount, JSON.stringify(myQOption));
            q1.bind(cons.exchangeParas.exRequest, pOption.replyTo);
            q1.subscribe({ack: true, prefetchCount: myPrefetchCount}, function(message, header, deliveryInfo, ack) {
                recvCallback(message, header, deliveryInfo, ack);
            });
        });
        connect.queue(serviceName, cons.queueParas.qOption, function (q2) {
            console.warn("[MQHD]Broadcast queue "+serviceName+" for reply is open.");
            q2.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
            q2.subscribe(function(message, header, deliveryInfo) {
                recvCallback(message, header, deliveryInfo);
            });
        });
        if (null == timerHandle) {
            timerHandle = setInterval(procMqKeepalive, 120000);
        }
    });

    connect.on('error', function (err) {
        console.error("[MQHD]Failed to connect MQ for "+serviceName+' with '+err+', mq option: '+JSON.stringify(mqOption));
    });
    connect.on('end', function (err) {
        console.warn("[MQHD]received end event.");
    });
    connect.on('close', function (err) {
        console.warn("[MQHD]received close event.");
        setTimeout(function() {
            connect.reconnect();
        }, 5000);
    });

    return this.connection = connect;
};

/*
 * 函数功能：对于需要确认的mq消息进行确认
 * 参数说明：ack   ack对象
 * 注意事项：该接口为具有ack机制的接口，这里的参数为connectReadyForServiceNeedAck的recvCallback的第四个参数ack
 *         必须显式的调用该函数以完成mq消息的确认，否则未响应的消息数达到prefetchCount后将无法再收到任何mq消息
 */
MqOper.prototype.ack = function(ack) {
    if (ack) {
        ack.acknowledge();
    } else {
        console.error('ack is null, please check it.');
    }
};

/*
 * 函数功能: connectReadyForService深度封装版。
 *         不关注header、deliveryInfo，只关注message的微服务可用该接口
 * 参数说明：1）serviceName： 微服务名
 *         2）msgProcFunc: 接收MQ消息的回调处理函数
 */
MqOper.prototype.connectReadyForServiceDep = function (serviceName, msgProcFunc) {
  var mqOption = this._genConnectOption(serviceName);

  // set 'replyTo' for reply queue name
  this.setSendOption('appId', serviceName);
  this.setSendOption('replyTo', [serviceName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

  var timerHandle = null;
  var connect = amqp.createConnection(mqOption, implOptions);
  connect.on('ready', function() {
    console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serviceName, JSON.stringify(mqOption));

    mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);   /* 声明 */
    mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);
    mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

    connect.queue(serviceName, cons.queueParas.qOption, function (q1) {
      console.warn("[MQHD]Queue " + serviceName + " for request is open.");
      q1.bind(cons.exchangeParas.exRequest, serviceName);
      q1.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
      q1.subscribe(function(message, header, deliveryInfo) {
        if (message.data) {
          var jsonBody = JSON.parse(message.data);
          var resMsg   = msgProcFunc(jsonBody);

          mqoper.replyMsg(resMsg, deliveryInfo);
        } else {
          console.error('[MQHD]The subscribed message.data is invalid, message: ' + JSON.stringify(message));
        }
      });
    });
      if (null == timerHandle) {
          timerHandle = setInterval(procMqKeepalive, 120000);
      }
  });

  connect.on('error', function (err) {
    console.error("[MQHD]Failed to connect MQ for "+serviceName+' with '+err+', mq option: '+JSON.stringify(mqOption));
  });
  connect.on('end', function (err) {
    console.warn("[MQHD]received end event.");
  });
  connect.on('close', function (err) {
    console.warn("[MQHD]received close event.");
      setTimeout(function() {
          connect.reconnect();
      }, 5000);
  });

  return this.connection = connect;
};

/*
 * 函数功能: 用于webserver连接MQ服务器。(chatserver也可用)
 * 参数说明：1）serverName： 微服务名，不用填写该参数（给chatserver用）
 *         2）recvCallback: 接收MQ消息的回调处理函数
 */
MqOper.prototype.connectReadyForServer = function (recvCallback, serverName) {
    if (undefined === serverName) {
        serverName = 'iotwebserver';
    }
    var mqOption = this._genConnectOption(serverName);

    // set 'replyTo' for reply queue name
    this.setSendOption('appId', serverName);
    this.setSendOption('replyTo', [serverName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

    var timerHandle = null;
    var connect = amqp.createConnection(mqOption, implOptions);
    connect.on('ready', function() {
        console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serverName, JSON.stringify(mqOption));

        mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);
        mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);   /* 声明 */
        mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

        var pOption = mqoper.getSendOption();
        connect.queue(pOption.replyTo, cons.queueParas.qOption, function (q1) {
            console.warn("[MQHD]Queue "+pOption.replyTo+" for reply is open.");
            q1.bind(cons.exchangeParas.exResponse, pOption.replyTo);
            q1.subscribe(function(message, header, deliveryInfo) {
                recvCallback(message, header, deliveryInfo);
            });
        });
        connect.queue(serverName, cons.queueParas.qOption, function (q2) {
            console.warn("[MQHD]Queue "+serverName+" for reply is open.");
            q2.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
            q2.subscribe(function(message, header, deliveryInfo) {
                recvCallback(message, header, deliveryInfo);
            });
        });
        if (null == timerHandle) {
            timerHandle = setInterval(procMqKeepalive, 120000);
        }
    });

    connect.on('error', function (err) {
        console.error('[MQHD]Failed to connect MQ for ' + serverName + ' with ' + err + ', mq option: ' +
                      JSON.stringify(mqOption));
    });
    connect.on('end', function (err) {
        console.warn("[MQHD]received end event.");
    });
    connect.on('close', function (err) {
        console.warn("[MQHD]received close event.");
        setTimeout(function() {
            connect.reconnect();
        }, 5000);
    });

    return this.connection = connect;
};

/*
 * 函数功能: 用于webserver连接MQ服务器(need ack)(chatserver也可用)
 * 参数说明：1）recvCallback: 接收MQ消息的回调处理函数
 *         2）qOption：见前面介绍
 *         3）serverName： 微服务名，不用填写该参数（给chatserver用）
 */
MqOper.prototype.connectReadyForServerNeedAck = function (recvCallback, qOption, serverName) {
    if (2 == arguments.length) {
        var para1 = arguments[1];
        if (typeof para1 === 'string') {
            qOption    = null;
            serverName = para1;
        }
    }
    if (undefined === serverName) {
        serverName = 'iotwebserver';
    }
    var myPrefetchCount = cons.defaultMQPrefetchCount;
    if (qOption && qOption.prefetchCount && qOption.prefetchCount > 0 && qOption.prefetchCount <= 2*myPrefetchCount) {
        myPrefetchCount = qOption.prefetchCount;
    }
    var myQOption = cons.queueParas.qOptionAck;
    if (qOption && qOption["x-max-length"] && qOption["x-max-length"] > 0 && qOption["x-max-length"] <= 2*myQOption.arguments["x-max-length"]) {
        myQOption.arguments["x-max-length"] = qOption["x-max-length"];
    }
    if (qOption && qOption["x-max-length-bytes"] && qOption["x-max-length-bytes"] > 0 && qOption["x-max-length-bytes"] <= 2*myQOption.arguments["x-max-length-bytes"]) {
        myQOption.arguments["x-max-length-bytes"] = qOption["x-max-length-bytes"];
    }
    console.warn('myQOption: ', JSON.stringify(myQOption));

    var mqOption = this._genConnectOption(serverName);

    // set 'replyTo' for reply queue name
    this.setSendOption('appId', serverName);
    this.setSendOption('replyTo', [serverName, basic.getLocalIP('eth', 'IPv4'), process.pid].join(':'));

    var timerHandle = null;
    var connect = amqp.createConnection(mqOption, implOptions);
    connect.on('ready', function() {
        console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serverName, JSON.stringify(mqOption));

        mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);
        mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);   /* 声明 */
        mqoper.broadcastReq = connect.exchange(cons.exchangeBroadcast.exRequest, cons.exchangeBroadcast.exFanoutOption);

        var pOption = mqoper.getSendOption();
        connect.queue(pOption.replyTo, myQOption, function (q1) {
            console.warn("[MQHD]Queue %s for reply is open, prefetchCount: %s, option: %s.", pOption.replyTo, myPrefetchCount, JSON.stringify(myQOption));
            q1.bind(cons.exchangeParas.exResponse, pOption.replyTo);
            q1.subscribe({ack: true, prefetchCount: myPrefetchCount}, function(message, header, deliveryInfo, ack) {
                recvCallback(message, header, deliveryInfo, ack);
            });
        });
        connect.queue(serverName, cons.queueParas.qOption, function (q2) {
            console.warn("[MQHD]Queue "+serverName+" for reply is open.");
            q2.bind(cons.exchangeBroadcast.exRequest, broadcast_key);
            q2.subscribe(function(message, header, deliveryInfo) {
                recvCallback(message, header, deliveryInfo);
            });
        });
        if (null == timerHandle) {
            timerHandle = setInterval(procMqKeepalive, 120000);
        }
    });

    connect.on('error', function (err) {
        console.error('[MQHD]Failed to connect MQ for ' + serverName + ' with ' + err + ', mq option: ' +
                      JSON.stringify(mqOption));
    });
    connect.on('end', function (err) {
        console.warn("[MQHD]received end event.");
    });
    connect.on('close', function (err) {
        console.warn("[MQHD]received close event.");
        setTimeout(function() {
            connect.reconnect();
        }, 5000);
    });

    return this.connection = connect;
};

/*
 * 函数功能: 用于有组播需求的微服务连接MQ服务器，比如chatserver
 * 参数说明：1）serviceName： 微服务名
 *         2）recvCallback: 接收MQ消息的回调处理函数
 * 注意事项：既作为'webserver'的微服务，又作为关联微服务的服务端，并且服务端实例间有组播需求
 */
MqOper.prototype.connectReadyForMultiServer = function (recvCallback, serverName) {
  if (undefined === serverName) {
    serverName = cons.serviceName.chatserver;
  }
  var mqOption = this._genConnectOption(serverName);
  var bindKey  = serverName + '.#';

  // set 'replyTo' for reply queue name
  this.location = [serverName, basic.getLocalIP('eth', 'IPv4'), process.pid].join('.');
  this.setSendOption('appId', serverName);
  this.setSendOption('replyTo', this.location);

  var timerHandle = null;
  var connect = amqp.createConnection(mqOption, implOptions);
  connect.on('ready', function() {
    console.warn((new Date()) + ' [MQHD]Connect to MQ for %s is ready: %s', serverName, JSON.stringify(mqOption));

    mqoper.exdirect = connect.exchange(cons.exchangeParas.exRequest, cons.exchangeParas.exDirOption);
    mqoper.exreply  = connect.exchange(cons.exchangeParas.exResponse, cons.exchangeParas.exDirOption);   /* 声明 */
    mqoper.exmultiReq = connect.exchange(cons.exchangeMulticast.exRequest, cons.exchangeMulticast.exOption);

    var pOption = mqoper.getSendOption();
    connect.queue(pOption.replyTo, cons.queueParas.qOption, function (q1) {
      console.warn("[MQHD]Queue " + pOption.replyTo + " for micro-services reply is open.");
      q1.bind(cons.exchangeParas.exResponse, pOption.replyTo);  // 微服务回应
      q1.bind(cons.exchangeMulticast.exRequest, bindKey);   // 组播收
      q1.subscribe(function(message, header, deliveryInfo) {
        recvCallback(message, header, deliveryInfo);
      });
    });
    connect.queue(serverName, cons.queueParas.qOption, function (q2) {
      console.warn("[MQHD]Queue " + serverName + " for received active-send msg is open.");
      q2.bind(cons.exchangeParas.exResponse, serverName);   // 微服务主动发
      q2.subscribe(function(message, header, deliveryInfo) {
        recvCallback(message, header, deliveryInfo);
      });
    });
      if (null == timerHandle) {
          timerHandle = setInterval(procMqKeepalive, 120000);
      }
  });

  connect.on('error', function (err) {
    console.error('[MQHD]Failed to connect MQ for ' + serverName + ' with ' + err + ', mq option: ' +
      JSON.stringify(mqOption));
  });
  connect.on('end', function (err) {
    console.warn("[MQHD]received end event.");
  });
  connect.on('close', function (err) {
    console.warn("[MQHD]received close event.");
      setTimeout(function() {
          connect.reconnect();
      }, 5000);
  });

  return this.connection = connect;
};

/*
 * 函数功能：连接V2-RabbitMQ服务器，针对V3通过MQ发消息到V2场景
 * 参数说明：connOptions 连接MQ服务器的参数  必选，形如：{ host: , port: 5672, login: , password: }
 */
MqOper.prototype.connectReadyForV2 = function (connOptions) {
  var timerHandle = null;
  var nodeEnv     = process.env;
  var mqOptions   = {};

  mqOptions.host     = nodeEnv.LVZHOUV2_CONFIG_MQ_HOST || connOptions.host;
  mqOptions.port     = nodeEnv.LVZHOUV2_CONFIG_MQ_PORT || connOptions.port || 5672;
  mqOptions.login    = nodeEnv.LVZHOUV2_CONFIG_MQ_USR  || connOptions.login;
  mqOptions.password = nodeEnv.LVZHOUV2_CONFIG_MQ_PWD  || connOptions.password;

  var connect = amqp.createConnection(mqOptions, implOptions);
  connect.on('ready', function() {
    console.log((new Date()) + ' [MQHD]Connect to MQ(V2) for %s is ready', JSON.stringify(mqOptions));

    if (null == timerHandle) {
      timerHandle = setInterval(procMqKeepalive, 120000);
    }
  });

  connect.on('error', function (err) {
    console.error("[MQHD]Failed to connect MQ(V2) with " + err + ", Options: " + JSON.stringify(mqOptions));
  });
  connect.on('end', function (err) {
    console.warn("[MQHD]received end event from MQ(V2).");
  });
  connect.on('close', function (err) {
    setTimeout(function() {
      connect.reconnect();
    }, 5000);

    console.warn("[MQHD]received close event from MQ(V2).");
  });

  return this.connectionV2 = connect;
};

/*
 * 函数功能：V3通过MQ发消息到V2
 * 参数说明：1）exchgName   发送消息的Exchange名称
 *         2）routingKey  发送消息的RoutingKey
 *         3）msg         待发送消息内容
 *         4）options     发送选项, 可选
 */
MqOper.prototype.sendMsgV2 = function (exchgName, routingKey, msg, options) {
  var exchgs = mqoper.exchgV2List;
  var sendExchg = exchgs[exchgName];

  options = options || {};
  if (!sendExchg) {
    var exOptions = {
      type: 'direct',
      durable: true,
      autoDelete: false
    //  confirm: true
    };

    sendExchg = exchgs[exchgName] = mqoper.connectionV2.exchange(exchgName, exOptions);
    sendExchg.on('open', function() {
      sendExchg.publish(routingKey, msg, options);
      console.log((new Date()) + ' [MQHD]Send message(V2) to exchange %s success with key %s.', exchgName, routingKey);
      console.log('  message: %s', JSON.stringify(msg));
      console.log('  options: %s.', JSON.stringify(options));
    });

    console.log("[MQHD]Create exchange(V2) ", exchgName + ", with options: " + JSON.stringify(exOptions));
  } else {
    sendExchg.publish(routingKey, msg, options);
    console.log((new Date()) + ' [MQHD]Send message(V2) to exchange %s success with key %s.', exchgName, routingKey);
    console.log('  message: %s', JSON.stringify(msg));
    console.log('  options: %s.', JSON.stringify(options));
  }
};

/*
 * msg: msg to send
 * Description: 用于实例间组播消息派发, 如chatserver
 * msg format: JSON格式的字符串
 * */
MqOper.prototype.multiSend = function (msg) {
  var sendExchage = this.exmultiReq;
  var option   = JSON.parse(JSON.stringify(this.getSendOption()));
  var multiKey = this.location;

  delete option["correlationId"];   // 避免sendMsg填的字段干扰

  if(sendExchage)
  {
    try {
      sendExchage.publish(multiKey, msg, option);
      console.log((new Date()) + ' [MQHD]Multicast message to %s success by %s.', multiKey, sendExchage.name);
      console.log('  message: %s', msg);
      console.log('  key/option = %s/%s.', multiKey, JSON.stringify(option));
    }
    catch(err) {
      console.error((new Date()) + ' [MQHD]Multicast message to %s failed with error: %s', multiKey, err);
    }
  } else {
    console.warn("[MQHD]Exchange for multicast is null or undefined!");
  }
};

/*
 * pkey: 发往的服务端名, 如chatserver, 也可指定服务端能收到的队列key名
 * msg: msg to send
 * Description: 用于微服务主动往服务端如chatServer发消息;
 * msg format: JSON格式的字符串
 * */
MqOper.prototype.activeSend = function (pkey, msg) {
  var sendExchage = this.exreply;
  var option = this.getSendOption();

  delete option["correlationId"];   // 避免sendMsg填的字段干扰

  if (sendExchage)
  {
    try {
      sendExchage.publish(pkey, msg, option);
      console.log((new Date()) + ' [MQHD]Active-send message to %s success by %s.', pkey, sendExchage.name);
      console.log('  message: %s', msg);
      console.log('  key/option = %s/%s.', pkey, JSON.stringify(option));
    }
    catch(err) {
      console.error((new Date()) + ' [MQHD]Active-send message to %s failed with error: %s', pkey, err);
    }
  } else {
    console.warn("[MQHD]Exchange for active-send is null or undefined!");
  }
};

var mqoper = module.exports = new MqOper;