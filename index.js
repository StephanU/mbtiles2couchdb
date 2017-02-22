if (process.argv.length !== 4) {
  console.log('Usage: node index.js <path to mbtiles file> <path to couchdb>')
  console.log('e.g. node index.js /Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles')
  throw Error
}

var url = require('url')
var async = require('async')
var sqlite3 = require('sqlite3')
var path = require('path')
var fs = require('fs')
var mbFilePath = path.resolve(__dirname, process.argv[2])
var couchdbUrl = url.parse(process.argv[3], false)
var nano = require('nano')(couchdbUrl.protocol + '//' + (couchdbUrl.auth ? couchdbUrl.auth + '@' : '') + couchdbUrl.host)
var couchdbName = couchdbUrl.path.substr(1)
var db

var paths = []

getAllMbtileFiles(mbFilePath)

nano.db.get(couchdbName, function (err) {
  if (err) {
    nano.db.create(couchdbName, function (err) {
      if (err) throw err
      mbtiles2couchdb(paths, couchdbName)
    })
  } else {
    mbtiles2couchdb(paths, couchdbName)
  }
})

function getAllMbtileFiles (mbFilePath) {
  // Check if mbFilePath is directory,
  // if so read all mbtile files (recursive) and fill an array with it's path, else create an array with one entry
  var stat = fs.statSync(mbFilePath)
  if (stat.isDirectory()) {
    var filesAndFoldersNames = fs.readdirSync(mbFilePath)
    for (var i = 0; i < filesAndFoldersNames.length; i++) {
      var name = filesAndFoldersNames[i]
      getAllMbtileFiles(path.resolve(mbFilePath, name))
    }
  } else {
    if (mbFilePath.indexOf('.mbtiles') > -1) paths.push(mbFilePath)
  }
}

function mbtiles2couchdb (mbFilePaths, couchdbName) {
  console.log('Start Uploading ' + mbFilePaths[0])
  var mbtilesDB = new sqlite3.Database(mbFilePaths[0], function (err) {
    db = nano.use(couchdbName)
    if (err) throw err
    mbtilesDB.get('SELECT count(*) as numrows from tiles', function (err, row) {
      if (err) throw err
      var rowCount = row.numrows
      var count = 0
      var queue = async.queue(upload, 20)

      mbtilesDB.each('SELECT * from tiles', function (err, row) {
        if (err) throw err
        queue.push(row, function (err) {
          if (err) console.log(err)
          process.stdout.write('\rUploaded tile ' + ++count + ' of ' + rowCount)
        })
      })

      queue.drain = function () {
        process.stdout.write('\nfinished' + '\n')
        mbFilePaths.shift()
        if (mbFilePaths.length > 0) mbtiles2couchdb(mbFilePaths, couchdbName)
      }
    })
  })
}

function upload (row, cb) {
  var tileRow = (1 << row.zoom_level) - 1 - row.tile_row
  var doc = {
    '_id': row.zoom_level + '_' + row.tile_column + '_' + tileRow,
    'zoom_level': row.zoom_level,
    'tile_column': row.tile_column,
    'tile_row': tileRow
  }
  var attachments = [{
    'name': 'tile.png',
    'data': row.tile_data,
    'content_type': 'image/png' // TODO determine actual content type of row.tile_data
  }]
  db.get(doc._id, function (err, response) {
    if (!err) {
      doc._rev = response._rev
    }
    db.multipart.insert(doc, attachments, doc._id, cb)
  })
}

