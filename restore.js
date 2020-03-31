const { MongoClient } = require('mongodb');
const fs = require('fs');
const es = require('event-stream');
const JSONStream = require('JSONStream');
let pendingTask = 0;
let completedTask = 0;

// readStream
//   .pipe(es.split())
//   .pipe(es.parse())
//   .pipe(
//     es.map(function(doc, next) {
//       pendingTask++;
//       setTimeout(() => {
//         completedTask++;
//         if (completedTask === pendingTask) {
//           console.log('done');
//         }
//       }, Math.ceil(Math.random() * 10000));
//       next();
//       // collection.insert(doc, next);
//     })
//   );
const host = 'localhost';
const port = '27017';
const username = '';
const password = '';
const dbName = 'testdb';
const url = `mongodb://${
  username && password ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@` : ''
}${host}:${port}/${dbName}`;

const rootDir = './backup/';

(async () => {
  const client = await getClient(url);
  const db = getDb(client, dbName);
  const collections = JSON.parse(fs.readFileSync(`${rootDir}collectionList.json`).toString());
  collections.forEach(({ name }) => {
    restoreCollections(db, name, client);
  });
})();

function getClient(url) {
  return new Promise((resolve, reject) => {
    MongoClient.connect(url, { useUnifiedTopology: true }, function(err, client) {
      if (err) {
        reject(err);
      }
      resolve(client);
    });
  });
}
function getDb(client, dbName) {
  return client.db(dbName);
}
function restoreCollections(db, collectionName, client) {
  const readStream = fs.createReadStream(`${rootDir}${collectionName}.json`);
  readStream.pipe(JSONStream.parse('*')).pipe(
    es.map(function(doc, next) {
      pendingTask += 1;
      delete doc._id;
      db.collection(collectionName).insertOne(doc, err => {
        if (err) throw err;
        completedTask += 1;
        if (completedTask === pendingTask) {
          console.log(`${completedTask} Document Restored`);
          client.close();
        }
      });
      next();
    })
  );
}
