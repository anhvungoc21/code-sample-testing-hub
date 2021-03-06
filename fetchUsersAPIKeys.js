const AWS = require("aws-sdk");
const { MongoClient } = require("mongodb");
const mongoDBURI = ""; // Deleted for data privacy and security reasons.
let db = null;
let cursor = null;
let myClient = null;

const connectToDatabase = async (uri, dbName) => {
  return MongoClient.connect(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  }).then((client) => {
    myClient = client;
    db = client.db(dbName);
    return db;
  });
};

const getAPIKeysByEmail = async (db, table) => {
  cursor = db.collection(table).find();
  const data = cursor.toArray();
  return data;
};

const lambda = new AWS.Lambda({
  region: "us-east-1",
});

const async_lambda_invoke = async (payload) => {
  const FunctionName = `${process.env.CHILD_FUNC_NAME}`;
  await lambda
    .invoke({
      FunctionName,
      InvocationType: "Event",
      Payload: JSON.stringify(payload),
    })
    .promise();
};

exports.handler = async (event) => {
  // Get URI & Database name
  const dbConnection = await connectToDatabase(mongoDBURI, "testinghub");
  const data = await getAPIKeysByEmail(dbConnection, "users");
  myClient.close();

  // Parse API Keys
  const apiKeys = [];
  data.forEach((doc) => {
    apiKeys.push(...doc.apiKeys);
  });

  // Run child lambda for each API key
  await Promise.all(
    apiKeys.map(async (apiKey) => {
      console.log(apiKey);
      const payload = { apiKey: apiKey }; // Payload is used to pass arguments
      await async_lambda_invoke(payload);
    })
  );

  return {
    statusCode: 200,
    body: JSON.stringify(apiKeys),
  };
};
