/**
 * 私有路由，转发http请求
 *
 */
const express = require('express')
const router = express.Router()


router.use((req, res, next) => {
  console.log('Access /api/ace router with req.url: ' + req.url)
  res.write(JSON.stringify({ code: 0, msg: '数据获取成功', data: '数据获取成功' }))
  res.end()
})

module.exports = router