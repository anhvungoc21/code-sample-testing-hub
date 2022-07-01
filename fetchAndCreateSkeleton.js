const fetch =  require("node-fetch");
const AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient({region: 'us-east-1'});

async function get_metrics_ID(apiKey) {
    const url = `https://a.klaviyo.com/api/v1/metrics?page=0&count=100&api_key=${apiKey}`;
    const options = { method: "GET", headers: { Accept: "application/json" } };

    return fetch(url, options)
      .then((res) => res.json())
      .then((json) => json)
      .catch((err) => console.error("error:" + err));
  }

async function extract_metrics_ID(apiKey, metricName) {
  let json = await get_metrics_ID(apiKey);
  let result;
  for (let i = 0; i < json.data.length; i++) {
    if (json.data[i].name == metricName) {
      result = json.data[i].id;
    }
  }
  return result;
}

function make_range(daysAgo) {
  const ret = [];
  for (let i = daysAgo; i > 0; i = i - 0.2) {
    ret.push(i);
  }
  return ret;
}

function promise_batch(apiKey, metricID, since) {
  const url = `https://a.klaviyo.com/api/v1/metric/${metricID}/timeline?since=${since}&count=100&sort=asc&api_key=${apiKey}`;
  const options = { method: "GET", headers: { Accept: "application/json" } };
  return fetch(url, options);
}

// Returns a promise if next is not null, or next is smaller than `until` checkpoint
function create_promise(apiKey, metricID, token, until) {
  // Check for null token
  if (token === null) return null;

  // Parse token for int comparison
  if (typeof token === "string") {
    // Token is a string (Returned by batch -- `next`)
    const tokenParsed = parseInt(token.split("-")[0], 10);
    if (tokenParsed > until) return null;
  } else {
    // Token is a number
    if (token > until) return null;
  }

  // Make promise
  const promise = promise_batch(apiKey, metricID, token);
  return promise;
}

const create_promise_arr = function (
  apiKey,
  metricID,
  tokenArr,
  checkpointArr
) {
  const promiseArr = [];

  // Indices to delete from tokenArray and checkpointArr when a promise returns null
  const indicesToDel = [];

  tokenArr.forEach((token, i) => {
    // Create new promise
    const newPromise = create_promise(
      apiKey,
      metricID,
      token,
      checkpointArr[i]
    );

    if (newPromise != null) {
      promiseArr.push(newPromise);
    } else {
      indicesToDel.push(i);
    }
  });

  // Update/shorten tokenArray and checkpointArr
  indicesToDel.forEach((i) => {
    tokenArr.splice(i, 1);
    checkpointArr.splice(i, 1);
  });

  return promiseArr;
};

const run_promise_arr = async function (apiKey, metricID, daysAgo) {
  let promiseArr;
  let responses;
  let nextArr;
  const result = {};

  // Making checkpoints and starting points for `next`
  const currentTime = Math.floor(new Date().getTime() / 1000);
  nextArr = make_range(daysAgo);
  let checkPoints = nextArr.map((cp) => cp - 0.2);

  checkPoints = checkPoints.map((cp) => currentTime - Math.floor(cp * 86400));
  nextArr = nextArr.map((next) => currentTime - Math.floor(next * 86400));

  while (true) {
    // checkPoints will be updated with the nextArr
    promiseArr = create_promise_arr(apiKey, metricID, nextArr, checkPoints); 

    // Check for stop here.
    if (promiseArr.length === 0) break;

    responses = await Promise.all(promiseArr);
    
    for (let i = 0; i < responses.length; i++) {
      let data = await responses[i].json();
      data.data.forEach((event) => {
        const eventID = event.event_properties.$event_id;
        result[eventID] = event; // Ensure events are unique
      });
      nextArr[i] = data.next;
    }
  }

  const retArr = Object.values(result);
  return retArr;
};

function write_to_ddb (apiKey, data, messageDict) {
  const params = {
      TableName: "SkeletonByAPIKeys",
      Item: {
          "apiKeys": apiKey,
          "data": data,
          "messageDict": messageDict
      },
  };
  
  return ddb.put(params).promise();
}

function extract_variations(data) {  
  // Returns
  let result = {};
  let messageDict = {};
  
  // Trash
  let trash = {};
  let undefinedFlowDict = {};
  
  for (let i = 0; i < data.length; i++) {
    let event = data[i];
    let properties = event.event_properties;
    let message = properties.$message;
    let message_name = properties["Campaign Name"];
    let variation = properties.$variation;
    let flow = properties.$flow;

    // Case 1: Has both flow and variation
    if (flow != undefined && variation != undefined) {
      if (flow in result) {
        if (message in result[flow]) {
          if (variation in result[flow][message]) {
            result[flow][message][variation] += 1;
          } else {
            result[flow][message][variation] = 1;
          }
        } else {
          result[flow][message] = {};
          result[flow][message][variation] = 1;
          messageDict[message] = message_name;
        }
      } else {
        result[flow] = {};
        result[flow][message] = {};
        result[flow][message][variation] = 1;
        messageDict[message] = message_name;
      }
    } else {
      // Case 2: Has flow but no variation
      if (flow != undefined) {
        if (!(flow in trash)) {
          trash[flow] = {};
        }
        trash[flow][message] = message_name;
      // Case 3: No flow. Don't store variation
      } else {
        undefinedFlowDict[message] = message_name;
      }
    }
  }
  return [result, messageDict];

}

exports.handler = async (event, context, callback) => {
  const apiKey = event.apiKey;
  const metricID = await extract_metrics_ID(apiKey, "Received Email");
  const daysAgo = 90;
  console.log("SUCCESSFULLY RAN FROM PARENT / TESTINGHUB", apiKey, metricID);
  const data = await run_promise_arr(apiKey, metricID, daysAgo);
  const [parsedData, messageDict] = extract_variations(data);
  
  await write_to_ddb(apiKey, parsedData, messageDict).then(() => {
    const writtenBody = {parsedData: parsedData, messageDict: messageDict}
    callback(null, {
        statusCode: 201,
        body: writtenBody,
        headers: {
            "Access-Control-Allow-Origin": "*"
        }
      });
    }).catch((err) => {
        console.log(err);
    });
};
