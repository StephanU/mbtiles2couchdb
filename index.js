if (process.argv.length !== 4) {
  console.log("Usage: node index.js <path to mbtiles file> <path to couchdb>");
  console.log("e.g. node index.js /Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles");
  return;
} 

var url = require('url'),
  async = require('async'),
  sqlite3 = require('sqlite3'),
  mbFilePath = process.argv[2],
  couchdbUrl = url.parse(process.argv[3], false),
  nano = require('nano')(couchdbUrl.protocol + '//' + (couchdbUrl.auth ? couchdbUrl.auth + '@' : '') + couchdbUrl.host),
  couchdbName = couchdbUrl.path.substr(1),
  db;

nano.db.get(couchdbName, function(err) {
  if (err) {
    nano.db.create(couchdbName, function(err) {
      if (err) throw err;
      mbtiles2couchdb(mbFilePath, couchdbName);
    });
  } else {
    mbtiles2couchdb(mbFilePath, couchdbName);
  }
});

function mbtiles2couchdb(mbFilePath, couchdbName) {
  var mbtilesDB = new sqlite3.Database(mbFilePath, function(err) {
    db = nano.use(couchdbName);
    if (err) throw err;
    mbtilesDB.get('SELECT count(*) as numrows from tiles', function(err, row) {
      if (err) throw err;
      var rowCount = row.numrows,
        count = 0,
        queue = async.queue(upload, 20);

      mbtilesDB.each('SELECT * from tiles', function(err, row) {
        if (err) throw err;
        queue.push(row, function(err) {
          if (err) throw err;
          console.log('Uploaded tile ' + ++count + ' of ' + rowCount);
        });
      });
    });
  });
}

function upload(row, cb) {
  var tile_row =  (1 << row.zoom_level) - 1 - row.tile_row,
    doc = {
      '_id': row.zoom_level + '_' + row.tile_column + '_' + tile_row,
      'zoom_level': row.zoom_level,
      'tile_column': row.tile_column,
      'tile_row': tile_row
    },
    attachments = [{
      'name': 'tile.png',
      'data': row.tile_data,
      'content_type': 'image/png' // TODO determine actual content type of row.tile_data
    }];

  db.multipart.insert(doc, attachments, doc._id, cb);
}
