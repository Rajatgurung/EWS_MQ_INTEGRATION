

const mqtt = require('mqtt');
const { processMessage } = require('./message-processor');
const { saveToDatabase } = require('./pg-db-handler');
const { processVolumeData } = require('./mongo-db-handler');

async function consumeWithWildcard(client, maxMessages = 100) {
  

  try {
    const mqttTopic = '#';
    
   
    console.log(`Subscribing to MQTT topic: ${mqttTopic}`);

    client.subscribe(mqttTopic, { qos: 1 }, (error) => {
      if (error) {
        console.error('Subscription error:', error);
        return;
      }
      
      console.log(`Successfully subscribed to topic: ${mqttTopic}`);
    });
    

    client.on('message', async (topic, message, packet) => {
      
      
      try {

        const records = [];

        const messageBody = message.toString('utf-8');

        const queueName = topic?.replaceAll(".","/");

        if(!queueName.includes("ews/pub")) return
        
        const processedRecords = processMessage(messageBody, queueName);
        records.push(...processedRecords);
        await saveToDatabase(records)
        if(records[0]?.minid == 52){
          await processVolumeData(records)
        }
      } catch (error) {
        console.error(`Error processing message:`, error);
      }
    });
    
    // Handle connection errors
    client.on('error', (error) => {
      console.error('MQTT client error:', error);
      
    });

    
  } catch (error) {
    console.error(`Error consuming with wildcard pattern:`, error);
  }
}

async function connectToMQTT(connectionString) {
  return new Promise((resolve, reject) => {
    try {
    
      console.log(`Connecting to MQTT broker`);
      
      const client = mqtt.connect(process.env.MQ_CONNECTION_URL,{
        port:8883,
        protocol: 'mqtts',
        username: process.env.MQ_USER,
        password: process.env.MQ_PASSWORD,
        ALPNProtocols: ['x-amzn-mqtt-ca']
      });
      
      client.on('connect', () => {
        console.log('Connected to MQTT broker');
        resolve(client);
      });
      
      client.on('error', (error) => {
        console.error('Error connecting to MQTT broker:', error);
        reject(error);
      });
      
      client.on('close', () => {
        console.log('MQTT connection closed');
      });
      
      client.on('offline', () => {
        console.log('MQTT client went offline');
      });
      
    } catch (error) {
      console.error('Error parsing connection string:', error);
      reject(error);
    }
  });
}

module.exports = {
  consumeWithWildcard,
  connectToMQTT
};
