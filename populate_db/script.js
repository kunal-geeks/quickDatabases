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
    const dataArr = [];
// Read and parse CSV file

fs.createReadStream(csvFilePath)
  .pipe(csvParser())
  .on('data', (row) => {
    dataArr.push(row);
  })
  .on('end', () => {
    const batches = []; // Array to hold the data
    const batchSize = 500; // Adjust the batch size as needed

    for (let i = 0; i < dataArr.length; i += batchSize) {
        const batch = db.batch();
        dataArr.slice(i,i+batchSize).forEach(record => {
            const docRef = collectionRef.doc();
            batch.set(docRef, record); 
        });
        batches.push(batch.commit());
    }
    // wait for all the batches to complete

    Promise.all(batches)
    .then(() => {
        console.log(`Firestore population completed!`);
        console.log(`CSV file read and parsed. Total records: ${dataArr.length}`);
    })
    .catch((error) => {
        console.log(`Error populating firestore: `,error)
    });
  });
};
populateFirestoreFromCsv(csvFilePath);