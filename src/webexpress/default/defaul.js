/** 
 * 默认路由处理，负责转发mq消息到对应服务
 * */
const express = require('express')
const router = express.Router()

router.use('(/api)?(/)?', (req, res, next) => {
  res.redirect('/api/web/demo')
})

module.exports = router
