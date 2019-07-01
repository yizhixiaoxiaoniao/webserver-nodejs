/**
 * 项目入口文件，初始化http服务和相关基础组件的连接
 * 
*/
const http = require('http')
const config = require('config')
const app = require('./src/webexpress/app')
// const dbhd = require('./src/lib/dboper')
// const mqhd = require('./src/lib/mqoper')

// 获取配置文件参数
const httpPort = config.get('httpPort')

// 2------ 初始化mongodb数据库连接
// dbhd.connectDatabase()

// 3------ 初始化mq服务器连接
// mqhd.setHostnames()
// mqhd.connectReadyForServer()

function timeProc() {
  // 1----- http服务启动
  const server = http.createServer(app)

  server.listen(httpPort, () => {
    console.log('Start http server with: ' + JSON.stringify(server.address()))
  })
}

// 延迟3s启动http服务
setTimeout(() => {
  timeProc()
}, 3 * 1000)