// Import necessary libraries
const fs = require('fs');
const csvParser = require('csv-parser');
const admin = require('firebase-admin');

// Initialize Firebase
const serviceAccount = require(`../firebase/practicalandquickdatabas-64452-firebase-adminsdk-4l2e4-0d853c694a.json`);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// Reference to your Firestore collection
const  collectionName = `yelp-coffee`;

// put your csv file path here
const csvFilePath = `../data/ratings_and_sentiments.csv`;

function populateFirestoreFromCsv(csvFilepath) {
    const db = admin.firestore();
    const collectionRef = db.collection(collectionName);
 
// Read and parse CSV file
const dataArr = []; // Array to hold the data
const batchSize = 500; // Adjust the batch size as needed

fs.createReadStream(csvFilePath)
  .pipe(csvParser())
  .on('data', (row) => {
    dataArr.push(row);
  })
  .on('end', () => {
    console.log(`CSV file read and parsed. Total records: ${dataArr.length}`);

    // Commit data in batches
    let startIndex = 0;
    while (startIndex < dataArr.length) {
      const batch = db.batch();
      const endIndex = Math.min(startIndex + batchSize, dataArr.length);

      for (let i = startIndex; i < endIndex; i++) {
        const docRef = collectionRef.doc();
        batch.set(docRef, dataArr[i]);
      }

      batch.commit()
        .then(() => {
          console.log(`Batch of ${endIndex - startIndex} documents written`);
        })
        .catch((error) => {
          console.error(`Error committing batch: ${error}`);
        });

      startIndex = endIndex;
    }
    console.log('All data batches committed');
  });
};
populateFirestoreFromCsv(csvFilePath);