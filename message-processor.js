
function extractUnitNumber(queueName) {
  try {
    const pattern = /ews\/pub\/(\d+)/;
    const match = queueName.match(pattern);
    if (match && match[1]) {
      return match[1];
    }
    return null;
  } catch (error) {
    console.error(`Error extracting unit number from ${queueName}:`, error);
    return null;
  }
}


function convertFieldValue(value) {
  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }
  
  if (typeof value === 'number') {
    return parseFloat(value);
  }
  
  if (typeof value === 'string') {
    const lowerValue = value.toLowerCase().trim();
    
    if (lowerValue === 'true' || lowerValue === '1' || lowerValue === 'yes' || lowerValue === 'on') {
      return 1;
    }
    if (lowerValue === 'false' || lowerValue === '0' || lowerValue === 'no' || lowerValue === 'off' || lowerValue === '') {
      return 0;
    }

    const parsed = parseFloat(value);
    if (!isNaN(parsed) && isFinite(parsed)) {
      return parsed;
    }
    
    console.warn(`Could not convert '${value}' to number, using 0.0`);
    return 0;
  }
  
  console.warn(`Unknown value type ${typeof value}: ${value}, using 0.0`);
  return 0;
}

function processMessage(messageBody, queueName) {
  try {
    const data = JSON.parse(messageBody);
    
    const unitNumber = extractUnitNumber(queueName);
    if (unitNumber === null) {
      console.error(`Could not extract unit number from queue: ${queueName}`);
      return [];
    }
    
    const messageType = data.messageType;
    const messageTime = data.messageTime;
    const dataArray = data.data || [];
    const messageId = Date.now()
    
    if (!messageType || !messageTime || !Array.isArray(dataArray) || dataArray.length === 0) {
      console.warn(`Missing required fields in message:`, data);
      return [];
    }
    
    const records = [];
    for (const item of dataArray) {
      const fieldName = item.fieldName;
      const fieldValue = item.fieldValue;
      
      if (fieldName === null || fieldName === undefined) {
        continue;
      }
      
      const numericValue = convertFieldValue(fieldValue);
      
      records.push({
        mobileId: unitNumber,
        time: messageTime,
        minid: messageType,
        fieldName: fieldName,
        fieldValue: numericValue,
        messageId
      });
    }
    
    return records;
  } catch (error) {
    if (error instanceof SyntaxError) {
      console.error(`Error parsing JSON message:`, error.message);
    } else {
      console.error(`Error processing message:`, error);
    }
    return [];
  }
}

module.exports = {
  extractUnitNumber,
  convertFieldValue,
  processMessage
};
