if (process.argv.length !== 4) {
  console.log('Usage: node index.js <path to mbtiles file> <path to couchdb>')
  console.log('e.g. node index.js /Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles')
  throw Error
}

const url = require('url')
const sqlite3 = require('sqlite3')
const path = require('path')
const fs = require('fs')
const mbFilePath = path.resolve(__dirname, process.argv[2])
const couchdbUrl = url.parse(process.argv[3], false)
const nano = require('nano')(
  couchdbUrl.protocol + '//' + (couchdbUrl.auth ? couchdbUrl.auth + '@' : '') + couchdbUrl.host
)
const couchdbName = couchdbUrl.path.substring(1)
const zlib = require('zlib')

const limit = 5000
const paths = getAllMbtileFiles(mbFilePath)

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

function getAllMbtileFiles(mbFilePath) {
  const paths = []
  // Check if mbFilePath is directory,
  // if so read all mbtile files (recursive) and fill an array with it's path, else create an array with one entry
  const stat = fs.statSync(mbFilePath)
  if (stat.isDirectory()) {
    var filesAndFoldersNames = fs.readdirSync(mbFilePath)
    for (let i = 0; i < filesAndFoldersNames.length; i++) {
      const name = filesAndFoldersNames[i]
      paths.push(getAllMbtileFiles(path.resolve(mbFilePath, name)))
    }
  } else {
    if (mbFilePath.indexOf('.mbtiles') > -1) {
      paths.push(mbFilePath)
    }
  }
  return paths
}

function getSQLRequest(lastZoomLevel, lastColumn, lastRow, limit) {
  return (
    `SELECT * from tiles 
          WHERE (zoom_level, tile_column, tile_row) > (` +
    lastZoomLevel +
    `,` +
    lastColumn +
    `,` +
    lastRow +
    `) 
          ORDER BY zoom_level, tile_column, tile_row
          LIMIT ` +
    limit +
    `;`
  )
}

function mbtiles2couchdb(mbFilePaths, couchdbName) {
  console.log('Start Uploading ' + mbFilePaths[0])
  const mbtilesDB = new sqlite3.Database(mbFilePaths[0], function (err) {
    const db = nano.use(couchdbName)
    if (err) throw err

    mbtilesDB.get('SELECT count(*) as numrows from tiles', function (err, row) {
      if (err) throw err
      const rowCount = row.numrows
      console.log(rowCount + ' tiles found')
      // console.log(new Date() + ' starting Upload')
      fetchAndPushToCouchAndRestart(-1, 0, 0, limit, db, mbtilesDB, 0)
    })
  })
}

function fetchAndPushToCouchAndRestart(lastZoomLevel, lastColumn, lastRow, limit, couchDB, sqlLite, count) {
  const mbtilesDB = sqlLite
  mbtilesDB.all(getSQLRequest(lastZoomLevel, lastColumn, lastRow, limit), function (err, sqliteRows) {
    if (sqliteRows.length === 0) return
    let ids = []
    for (let i = 0; i < sqliteRows.length; i++) {
      const row = sqliteRows[i]
      if (err) throw err
      const tileRow = (1 << row.zoom_level) - 1 - row.tile_row
      ids.push(row.zoom_level + '_' + row.tile_column + '_' + tileRow)
    }
    // console.log(new Date() + ' finished sqlite fetch')
    couchDB.fetchRevs({ keys: ids }, function (err, response) {
      if (err) throw err
      // console.log(new Date() + ' finished fetch revs')
      const bulkDocs = { docs: [] }
      for (let i = 0; i < response.rows.length; i++) {
        const rev = response.rows[i].error ? null : response.rows[i].value.rev
        bulkDocs.docs.push(getCouchDbDoc(sqliteRows[i], rev))
      }
      couchDB.bulk(bulkDocs, function (error, body) {
        if (error) throw error
        for (let i = 0; i < body.length; i++) {
          if (body[i].error) throw body[i].error
        }
        // console.log(new Date() + ' finished bulk')
        const newCount = count + body.length
        process.stdout.write('\r' + newCount + ' pushed')
        if (sqliteRows.length === limit) {
          const newLastZoom = sqliteRows[sqliteRows.length - 1].zoom_level
          const newLastColumn = sqliteRows[sqliteRows.length - 1].tile_column
          const newLastRow = sqliteRows[sqliteRows.length - 1].tile_row
          fetchAndPushToCouchAndRestart(newLastZoom, newLastColumn, newLastRow, limit, couchDB, sqlLite, newCount)
        } else {
          console.log(' finished')
        }
      })
    })
  })
}

function getCouchDbDoc(row, rev) {
  const tileRow = (1 << row.zoom_level) - 1 - row.tile_row
  const doc = {
    _id: row.zoom_level + '_' + row.tile_column + '_' + tileRow,
    zoom_level: row.zoom_level,
    tile_column: row.tile_column,
    tile_row: tileRow,
    type: 'map_tile@1.0.0',
    _attachments: {
      'tile.pbf': {
        data: zlib.unzipSync(row.tile_data).toString('base64'),
        content_type: 'application/x-protobuf' // TODO determine actual content type of row.tile_data
      }
    }
  }
  if (rev) doc._rev = rev
  return doc
}
