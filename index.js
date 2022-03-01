const {Storage} = require('@google-cloud/storage');

const storage = new Storage();

const sourceBucket = storage.bucket('iupui-cit41200-class-malpdf-pdf-source-test');

async function scanFiles () {
  const [files] = await sourceBucket.getFiles();

  files.forEach(file => {
    console.log(`File name: ${file.name}`);
  });
  
};

scanFiles();