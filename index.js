if (process.argv.length !== 4) {
  console.log("Usage: node index.js mbtiles://<path to mbtiles file> <path to couchdb>");
  console.log("e.g. node index.js mbtiles:///Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles");
  return;
} 

var url = require('url'),
  sqlite3 = require('sqlite3'),
  mbFileUrl = url.parse(process.argv[2], false),
  couchdbUrl = url.parse(process.argv[3], false),
  nano = require('nano')(couchdbUrl.protocol + '//' + (couchdbUrl.auth ? couchdbUrl.auth + '@' : '') + couchdbUrl.host),
  couchdbName = couchdbUrl.path.substr(1),
  rowCount;

nano.db.get(couchdbName, function(err) {
  if (err) {
    nano.db.create(couchdbName, function(err) {
      if (err) throw err;
      mbtiles2couchdb(mbFileUrl, couchdbName);
    });
  } else {
    mbtiles2couchdb(mbFileUrl, couchdbName);
  }
});

function mbtiles2couchdb(mbFileUrl, couchdbName) {
  var mbtilesDB = new sqlite3.Database(mbFileUrl.pathname, function(err) {
    if (err) throw err;
    mbtilesDB.get('SELECT count(*) as numrows from tiles', function(err, row) {
      if (err) throw err;
      var rowCount = row.numrows,
        count = 0,
        couchdb = nano.use(couchdbName);

        mbtilesDB.each('SELECT * from tiles', function(err, row) {
          if (err) throw err;
          upload(couchdb, row, function(err) {
            if (err) throw err;
            console.log('Uploaded tile ' + ++count + ' of ' + rowCount);
          });
        });
    });
  });
}

function upload(couchdb, row, cb) {
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

  couchdb.multipart.insert(doc, attachments, doc._id, cb);
}
