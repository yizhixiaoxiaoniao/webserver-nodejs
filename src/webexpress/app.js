/**
 * 路由入口文件
 * */
const express = require('express')
const cookieParser = require('cookie-parser');
const bodyParser = require('body-parser')
const session = require('express-session')
const config = require('config')
const ace = require('./api/ace/routes/index')
const web = require('./api/web/routes/index')
const defaul = require('./default/defaul')
const url = require('url')
const path = require('path')

// 实例化express框架
const app = express()

// 使用第三方中间件
app.use(cookieParser())
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))

const sess = {
  secret: 'demo secret',
  saveUninitialized: false,
  resave : true,
  rolling: true,
  cookie: { maxAge: config.get('cookieAge'), secure: true }
}

// 初始化session会话
app.use(session(sess))

// 拦截所有请求，进行res header设置
app.use((req, res, next) => {
  console.log('Access Router wth req.url: ' + req.url)
  if(req.session) {
    req.session['sso_user'] = 'jinfeng'
    req.session.cookie.maxAge = config.get('cookieAge')
  }
  next()
})

// auth认证处理
// app.use(auth) // TODO

// Mock假数据路由处理
app.use('/api/getTable1Data', (req, res, next) => {
  const query = url.parse(req.url, true).query
  const { pageSize, pageNum } = query
  console.log('getTable1Data with pageSize: s%, pageNum: s%', pageSize, pageNum)
  let data = [
      { userName: 'jinfeng', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng2', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng3', isAdmin: 'no', boardCount: '110', cardCount: '220', isEmail: 'no' },
      { userName: 'jinfeng4', isAdmin: 'yes', boardCount: '1', cardCount: '3', isEmail: 'yes' },
      { userName: 'jinfeng5', isAdmin: 'no', boardCount: '20', cardCount: '30', isEmail: 'no' },
      { userName: 'jinfeng', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng2', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng3', isAdmin: 'no', boardCount: '110', cardCount: '220', isEmail: 'no' },
      { userName: 'jinfeng4', isAdmin: 'yes', boardCount: '1', cardCount: '3', isEmail: 'yes' },
      { userName: 'jinfeng5', isAdmin: 'no', boardCount: '20', cardCount: '30', isEmail: 'no' },
      { userName: 'jinfeng', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng2', isAdmin: 'yes', boardCount: '10', cardCount: '20', isEmail: 'no' },
      { userName: 'jinfeng3', isAdmin: 'no', boardCount: '110', cardCount: '220', isEmail: 'no' },
      { userName: 'jinfeng4', isAdmin: 'yes', boardCount: '1', cardCount: '3', isEmail: 'yes' },
      { userName: 'jinfeng5', isAdmin: 'no', boardCount: '20', cardCount: '30', isEmail: 'no' }
  ]
  const resp = {
    code: 0,
    msg: '数据获取成功',
    data: {
        total: data.length,
        table1Data: data.slice(pageSize * (pageNum - 1), pageSize * pageNum)
    }
  }
  res.setHeader('Content-Type', 'application/json;charset=utf-8')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Progma', 'no-cache')
  res.write(JSON.stringify(resp))
  res.end()
})

// 使用express.static中间件，设置静态文件访问路径为localfs
app.use(express.static(path.join(__dirname, '../localfs')))

// 私有路由处理
app.use('/api/ace', ace)
app.use('/api/web', web)

// 默认路由处理
app.use(defaul)

module.exports = app
