const { MongoClient,ObjectId } = require('mongodb');

let client = null;
let db = null;


function getMongoClient() {
  if (!client) {
    const mongoUrl = process.env.MONGO_URL;
    
    if (!mongoUrl) {
      throw new Error('Missing required environment variable: MONGO_URL or MONGODB_URI');
    }
    
    const options = {
      maxPoolSize: 10,
      minPoolSize: 2,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    };
    
    client = new MongoClient(mongoUrl, options);
    

    client.on('error', (err) => {
      console.error('MongoDB client error:', err);
    });
    
    console.log('MongoDB client created');
  }
  
  return client;
}


async function connectToDatabase() {
  if (!db) {
    const mongoClient = getMongoClient();
    
    try {
      await mongoClient.connect();
      db = mongoClient.db();
      console.log(`Connected to MongoDB database: ${db.databaseName}`);
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
      throw error;
    }
  }
  
  return db;
}


async function getDatabase() {
  if (!db) {
    await connectToDatabase();
  }
  return db;
}


const getPreviousValue = (key, currentData, previousData) => {
    if ((+currentData[key] - +(previousData[key] || 0)) < 0) {
      return 0
    }
    return previousData[key]
  }
  

async function processVolumeData( data) {
  if (!data || (Array.isArray(data) && data.length === 0)) {
    return;
  }
  
 
  try {

    const brand = new ObjectId(process.env.BRAND)

    const db = await getDatabase();

    const volumeData = data.reduce((acc,d) => ({...acc,[d.fieldName]:d.fieldValue}),{})

    const mobileId= data[0]?.mobileId
  
    const asset = await db.collection("serializedAsset").findOne({brand,assetNumber:mobileId}) || {}

    const previousData = (await db.collection("rentalUnitVolumeData").find({asset:asset._id}).sort({ date: -1 }).limit(1).toArray()).pop() || {};


    data.forEach(element => {
        volumeData[`Previous${element.fieldName}`] = getPreviousValue(element.fieldName,volumeData,previousData)
    });
    

    await db.collection("rentalUnitVolumeData").insertOne({
        _id:new ObjectId(),
        ...volumeData,
        source:"MQ",
        date:new Date(data[0]?.time),
        asset:asset._id,
        brand,
        mobileId,
        source:"MQ"
    })
   
  } catch (error) {
    console.error(`Error saving to collection:`, error);
    throw error;
  }
}



module.exports = {
  getMongoClient,
  connectToDatabase,
  getDatabase,
  processVolumeData
};
