
const { Pool } = require('pg');
const format = require('pg-format')

let pool = null;

function getDbPool() {
  if (!pool) {
    const dbUrl = process.env.DB_URL || process.env.DATABASE_URL;
    
    if (!dbUrl) {
      throw new Error('Missing required environment variable: DB_URL or DATABASE_URL');
    }
    
    const config = {
      connectionString: dbUrl,
      max: 5, // Maximum number of clients in the pool
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    };
    
    pool = new Pool(config);
    
    // Handle pool errors
    pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
    });
    
    console.log('Database connection pool created');
  }
  
  return pool;
}

async function saveToDatabase(records) {
  if (!records || records.length === 0) {
    return;
  }
  
  const dbPool = getDbPool();
  const client = await dbPool.connect();
  
  try {
    await client.query('BEGIN');

    
    const values = records.map(r => [
      new Date(r.time),
      r.mobileId,
      r.minid,
      r.fieldName,
      r.fieldValue,
      r.messageId,
      r.time,
      "MQ"
    ]);
    

    const insertQuery = format('INSERT INTO iot ("time","mobileId", "minid", "fieldName", "fieldValue", "messageId", "receiveUTC", "source") VALUES %L', values);
    
    await client.query(insertQuery);
    await client.query('COMMIT');
    
    console.log(`Successfully inserted ${records.length} records`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error saving to database:', error);
    throw error;
  } finally {
    client.release();
  }
}



module.exports = {
  getDbPool,
  saveToDatabase
};
