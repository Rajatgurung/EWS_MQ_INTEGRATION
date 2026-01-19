
const { consumeWithWildcard, connectToMQTT} = require('./mq-consumer');
const { saveToDatabase, closeDbPool } = require('./pg-db-handler');
const { processMessage } = require('./message-processor');
const dotenv = require("dotenv")

const main = async () => {
  dotenv.config()
  let client = null;
  
  try {
    const connectionString = process.env.MQ_CONNECTION_URL;
    const maxMessages = parseInt(process.env.MAX_MESSAGES_PER_QUEUE || '100', 10);
    
    if (!connectionString) {
      throw new Error('Missing required environment variable: MQ_CONNECTION_URL');
    }
    
    if (!process.env.DB_URL && !process.env.DATABASE_URL) {
      throw new Error('Missing required database environment variable: DB_URL or DATABASE_URL');
    }
    
    console.log('Consuming messages from MQTT broker');
  
    client = await connectToMQTT(connectionString);
    await consumeWithWildcard(client, maxMessages);
  
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
