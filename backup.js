const { MongoClient } = require('mongodb');
const fs = require('fs');

const host = 'localhost';
const port = '27017';
const username = '';
const password = '';
const dbName = 'testdb';

const rootDir = './backup/';
if (!fs.existsSync(rootDir)) {
  fs.mkdirSync(rootDir);
}
const url = `mongodb://${
  username && password ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@` : ''
}${host}:${port}/${dbName}`;

(async () => {
  const client = await getClient(url);
  const db = getDb(client, dbName);
  const collections = await getCollections(db);
  makeCollectionList(collections);
  const collectionStreams = collections.map(({ name }) => getCollectionStream(db, name));
  const collectionBackupPromises = collectionStreams.map((stream, i) =>
    backupCollectionAsJson(stream, collections[i].name)
  );
  await Promise.all(collectionBackupPromises);
  client.close();
})();

function makeCollectionList(collectionsData) {
  fs.writeFile(`${rootDir}collectionList.json`, JSON.stringify(collectionsData), err => {
    if (err) throw err;
  });
}

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
function getCollections(db) {
  return new Promise((resolve, reject) => {
    return db.listCollections().toArray((err, collections) => {
      if (err) reject(err);
      resolve(collections);
    });
  });
}
function getCollectionStream(db, collectionName) {
  return db
    .collection(collectionName)
    .find({})
    .stream();
}
function backupCollectionAsJson(collectionStream, collectionsName) {
  return new Promise((resolve, reject) => {
    let firstEntry = true;
    fs.writeFileSync(`${rootDir}${collectionsName}.json`, '[', 'utf8');
    collectionStream.on('data', document => {
      let documentString = firstEntry
        ? ((firstEntry = false), JSON.stringify(document))
        : ',' + JSON.stringify(document);

      fs.appendFile(`${rootDir}${collectionsName}.json`, documentString, 'utf8', err => {
        if (err) reject(err);
      });
    });
    collectionStream.on('end', () => {
      fs.appendFile(`${rootDir}${collectionsName}.json`, ']', 'utf8', () => {
        resolve();
      });
    });
  });
}

// NSL=>'newline separated list'

function backupCollectionAsNSL(collectionStream, collectionsName) {
  return new Promise((resolve, reject) => {
    collectionStream.on('data', document => {
      fs.appendFile(`${rootDir}${collectionsName}.nsl`, JSON.stringify(document) + '\n', 'utf8', err => {
        if (err) reject(err);
      });
    });
    collectionStream.on('end', () => {
      resolve();
    });
  });
}
