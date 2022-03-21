const {Storage} = require('@google-cloud/storage');
const path = require('path');
const fs = require('fs-extra');
const os = require('os');
const csv = require('csv-parser');
const getExif = require('exif-async');
const parseDMS = require('parse-dms');
const {BigQuery} = require('@google-cloud/bigquery');

const bq = new BigQuery();

  exports.bucketReader = async (file, context) => {
    console.log(`Function version 1.0`);
    const gcsFile = file;
    console.log(`Processing file: ${gcsFile.name}`);
    const storage = new Storage();
    const sourceBucket = storage.bucket(file.bucket);
    const benignBucket = storage.bucket('sp22-cit41200-class-malpdf-pdf-benign');
    const maliciousBucket = storage.bucket('sp22-cit41200-class-malpdf-pdf-malicious');
   // Create a working directory on the GCF's VM to download the original file
        // The value of this variable will be something like `~/tmp/exif`
        const workingDir = path.join(os.tmpdir(), 'exif');
      
        // Create a variable that holds the path to the 'local' version of the file
        // The value of this variable will be something like `~/tmp/exif/1982746536475869.jpg`
        const tempFilePath = path.join(workingDir, gcsFile.name);
        console.log(`tempFilePath: ${tempFilePath}`);
      
        // Wait until the working directory is ready
        await fs.ensureDir(workingDir);
        console.log('Working directory is ready.');
      
        // Download the original file to the path on the 'local' VM
        await sourceBucket.file(gcsFile.name).download({
          destination: tempFilePath
        });
        console.log(`File downloaded: ${tempFilePath}`);
        console.log(`FILENAME: ${gcsFile.name}`);

        const getFileNameClass = async () => {
            // The SQL query to run
            
              const sqlQuery = "SELECT class FROM `sp22-41200-rsemla-class-pdf.PDF_MALWARE.PDF_MALWARE_TABLE` where filename||'.pdf' = @filename  LIMIT 1000";
            const options = {
              query: sqlQuery,
              location: 'US',
              params: {filename: gcsFile.name},
            };
          
            // Run the query
            const [rows] = await bq.query(options);
            // rows.forEach(row => console.log(row));
            
         
            rows.forEach(row => {
            if (`${row.class}` === 'Malicious'){
                maliciousBucket.upload(tempFilePath);
                console.log(`Malicious File uploaded.`);
             } else {
                benignBucket.upload(tempFilePath);
                 console.log(`Benign File uploaded.`);
             }
            });

        }
          
          getFileNameClass(gcsFile.name);
          
       console.log(`FILENAME: ${gcsFile.name}`);
       
       
        
        await sourceBucket.file(gcsFile.name).delete();
        console.log(`Deleting pdf file after upload: ${gcsFile.name} content type: ${gcsFile.contentType}`);
      
    };
  