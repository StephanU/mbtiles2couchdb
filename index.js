if (process.argv.length !== 4) {
  console.log('Usage: node index.js <path to mbtiles file> <path to couchdb>')
  console.log('e.g. node index.js /Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles')
  throw Error
}

var url = require('url')
var sqlite3 = require('sqlite3')
var path = require('path')
var fs = require('fs')
var mbFilePath = path.resolve(__dirname, process.argv[2])
var couchdbUrl = url.parse(process.argv[3], false)
var nano = require('nano')(couchdbUrl.protocol + '//' + (couchdbUrl.auth ? couchdbUrl.auth + '@' : '') + couchdbUrl.host)
var couchdbName = couchdbUrl.path.substr(1)
var zlib = require('zlib')

var db
var paths = []

var limit = 5000

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

function getSQLRequest (lastZoomLevel, lastColumn, lastRow, limit) {
  return `SELECT * from tiles 
          WHERE (zoom_level, tile_column, tile_row) > (` + lastZoomLevel + `,` + lastColumn + `,` + lastRow + `) 
          ORDER BY zoom_level, tile_column, tile_row
          LIMIT ` + limit + `;`
}

function mbtiles2couchdb (mbFilePaths, couchdbName) {
  console.log('Start Uploading ' + mbFilePaths[0])
  var mbtilesDB = new sqlite3.Database(mbFilePaths[0], function (err) {
    db = nano.use(couchdbName)
    if (err) throw err

    mbtilesDB.get('SELECT count(*) as numrows from tiles', function (err, row) {
      if (err) throw err
      var rowCount = row.numrows
      console.log(rowCount + ' tiles found')
     // console.log(new Date() + ' starting Upload')
      fetchAndPushToCouchAndRestart(-1, 0, 0, limit, db, mbtilesDB, 0)
    })
  })
}

function fetchAndPushToCouchAndRestart (lastZoomLevel, lastColumn, lastRow, limit, couchDB, sqlLite, count) {
  var mbtilesDB = sqlLite
  var db = couchDB
  mbtilesDB.all(getSQLRequest(lastZoomLevel, lastColumn, lastRow, limit), function (err, sqliteRows) {
    if (sqliteRows.length === 0) return
    var ids = []
    for (var i = 0; i < sqliteRows.length; i++) {
      var row = sqliteRows[i]
      if (err) throw err
      var tileRow = (1 << row.zoom_level) - 1 - row.tile_row
      ids.push(row.zoom_level + '_' + row.tile_column + '_' + tileRow)
    }
   // console.log(new Date() + ' finished sqlite fetch')
    db.fetchRevs({keys: ids}, function (err, response) {
      if (err) throw err
     // console.log(new Date() + ' finished fetch revs')
      var bulkDocs = {'docs': []}
      for (var i = 0; i < response.rows.length; i++) {
        var rev = (response.rows[i].error) ? null : response.rows[i].value.rev
        bulkDocs.docs.push(getCouchDbDoc(sqliteRows[i], rev))
      }
      db.bulk(bulkDocs, function (err, body) {
        if (err) throw err
       // console.log(new Date() + ' finished bulk')
        var newCount = count + body.length
        // process.stdout.write('\r' + newCount + ' pushed')
        if (sqliteRows.length === limit) {
          var newLastZoom = sqliteRows[sqliteRows.length - 1].zoom_level
          var newLastColumn = sqliteRows[sqliteRows.length - 1].tile_column
          var newLastRow = sqliteRows[sqliteRows.length - 1].tile_row
          fetchAndPushToCouchAndRestart(newLastZoom, newLastColumn, newLastRow, limit, couchDB, sqlLite, newCount)
        } else {
          console.log('finished')
        }
      })
    })
  })
}

function getCouchDbDoc (row, rev) {
  var tileRow = (1 << row.zoom_level) - 1 - row.tile_row

  var attachments = [{
    'data': zlib.unzipSync(row.tile_data),
    'content_type': 'application/x-protobuf' // TODO determine actual content type of row.tile_data
  }]
  var doc = {
    '_id': row.zoom_level + '_' + row.tile_column + '_' + tileRow,
    'zoom_level': row.zoom_level,
    'tile_column': row.tile_column,
    'tile_row': tileRow,
    '_attachments': {
      'tile.pbf': {
        'data': zlib.unzipSync(row.tile_data).toString('base64'),
        'content_type': 'application/x-protobuf' // TODO determine actual content type of row.tile_data
      }
    }
  }
  if (rev) doc._rev = rev
  return doc
}
