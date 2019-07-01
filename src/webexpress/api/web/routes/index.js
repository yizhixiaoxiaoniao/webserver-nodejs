/**
 * 页面访问路由处理
 *  */
const express = require('express')
const router = express.Router()
const path = require('path')
const fs = require('fs-extra')

router.use('/', (req, res, next) => {
  res.sendFile('/error/404.html', {root: path.resolve(__dirname, '../../../../localfs')})
})

router.use('/:scene', (req, res, next) => {
  const { scene } = req.params
  fs.exists(path.join(__dirname, '../../../../localfs/api/web/' + scene + '/index.html'), (err, exists) => {
    if(!err && exists) {
      res.sendFile('/api/web/' + scene + '/index.html', {root: path.resolve(__dirname, '../../../../localfs')})
    } else {
      res.sendFile('/error/404.html', {root: path.resolve(__dirname, '../../../../localfs')})
    }
  })
})


module.exports = router
