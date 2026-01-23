
const { consumeWithWildcard, connectToMQTT} = require('./mq-consumer');
const { connectToDatabase } = require('./mongo-db-handler');
const { getDbPool } = require('./pg-db-handler');
const { processMessage } = require('./message-processor');
const dotenv = require("dotenv")



const main = async () => {
  dotenv.config()
  let client = null;
  
  try {
    await connectToDatabase()
    await getDbPool()

    const connectionString = process.env.MQ_CONNECTION_URL;
    const maxMessages = parseInt(process.env.MAX_MESSAGES_PER_QUEUE || '100', 10);
    
    if (!connectionString) {
      throw new Error('Missing required environment variable: MQ_CONNECTION_URL');
    }
    
    if (!process.env.DB_URL && !process.env.DATABASE_URL) {
      throw new Error('Missing required database environment variable: DB_URL or DATABASE_URL');
    }

    if(!process.env.BRAND){
      throw new Error('Missing Brand');
    }
    if(!process.env.MONGO_URL){
      throw new Error('Missing required environment variable: MONGO_URL');
    }
    
    console.log('Consuming messages from MQTT broker');
  
    client = await connectToMQTT(connectionString);
    await consumeWithWildcard(client, maxMessages);

    console.log("\n\n","========================================","\n")
    console.log(' Server is up and running!');
    console.log("\n","========================================","\n\n")
  } catch (error) {
    console.error('Error:', error);
    
    // Ensure connection is closed on error
    if (client) {
      try {
        client.end();
      } catch (e) {
        console.error('Error disconnecting:', e.message);
      }
    }
  }
};

main()
.catch(e =>{
  console.log(e)
  process.exit()
}
)
