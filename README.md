# mbtiles2couchdb

Uploads tiles from an mbtiles file to couchdb. The couchdb database will be created if it doesn't exist. The script assumes an empty database. No fancy error checking implemented...

## Usage

```
npm install
node index.js mbtiles:///Users/Stephan/OSM.mbtiles http://name:password@localhost:5984/tiles
```

Tiles URL: http://localhost:5984/tiles/{z}\_{x}\_{y}/tile.png
