var config = require('config');
var cons   = require('./constants');

/*
 * 函数功能：获取hdfs参数信息。以环境变量的设置为准，如果环境变量未指定，则以配置文件为准
 * 参数说明：hdfsConfOpt 配置文件里的hdfs配置参数
 */
function getHdfsOption(hdfsConfOpt) {
    var hdfsOption = cons.defaultHdfsOption;
    if (hdfsConfOpt && config.has(hdfsConfOpt)) {
        hdfsOption = config.get(hdfsConfOpt);
    }
    var retOption = {user:hdfsOption.user, host:hdfsOption.host, port:hdfsOption.port, path:hdfsOption.path};
    retOption.user = process.env.LVZHOUV3_CONFIG_HDFS_USER || retOption.user;
    retOption.host = process.env.LVZHOUV3_CONFIG_HDFS_HOST || retOption.host;
    retOption.port = process.env.LVZHOUV3_CONFIG_HDFS_PORT || retOption.port;
    retOption.path = process.env.LVZHOUV3_CONFIG_HDFS_PATH || retOption.path;
    console.warn('[IOTPUB] hdfs conn option: %s', JSON.stringify(retOption));

    return retOption;
}

exports.getHdfsOption = getHdfsOption;