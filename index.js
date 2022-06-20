import fetch from "node-fetch";
import AWS from "aws-sdk";

export default async function handler(req, res) {
  const privateApiKey = req.body.apiKey;
  const metric = req.body.metric;
  const daysAgo = req.body.daysAgo;

  async function get_metrics_ID(apiKey) {
    const url = `https://a.klaviyo.com/api/v1/metrics?page=0&count=50&api_key=${apiKey}`;
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

  async function fetch_variation_counts(apiKey, metricID, startDate, endDate) {
    const url = `https://a.klaviyo.com/api/v1/metric/${metricID}/export?start_date=${startDate}&end_date=${endDate}&unit=day&measurement=count&by=%24variation&count=1000&api_key=${apiKey}`;
    const options = { method: "GET", headers: { Accept: "application/json" } };
    const regex = /\(\w{6}\)/;

    return await fetch(url, options)
      .then((res) => res.json())
      .then((json) => {
        const results = json.results;
        const varDict = {};
        results.forEach((record) => {
          const varID = String(record.segment.match(regex)).slice(1, 7);
          const varCountRecords = record.data;
          const varTotalCount = varCountRecords.reduce((sum, subRecord) => {
            return sum + subRecord.values[0];
          }, 0);
          varDict[varID] = varTotalCount;
        });
        return varDict;
      })
      .catch((err) => console.error("ERROR FETCHING:" + err));
  }

  function significance(var1_met, var1_d, var2_met, var2_d) {
    let p_winner;
    let p_loser;
    let winner;
    let winner_d;
    let loser_d;
    let crit_z = 1.96;
    if (var1_met < 30 || var2_met < 30) {
      return -1;
    }
    if (var1_met / var1_d > var2_met / var2_d) {
      p_winner = var1_met / var1_d;
      winner_d = var1_d;
      p_loser = var2_met / var2_d;
      loser_d = var2_d;
      winner = 1;
    } else {
      p_winner = var2_met / var2_d;
      winner_d = var2_d;
      p_loser = var1_met / var1_d;
      loser_d = var1_d;
      winner = 2;
    }
    //let p_total = (var1_met + var2_met) / (var1_d + var2_d)
    let z =
      (p_winner - p_loser) /
      Math.sqrt(
        (p_winner * (1 - p_winner)) / winner_d +
          (p_loser * (1 - p_loser)) / loser_d
      );

    if (z > crit_z) {
      return winner;
    }
    return -1;
  }

  /*
    Returns [metric_var1_count, received_var1_count, metric_var2_count, received_var2_count, winner, loser]
        Variable name is metric_count because metric = {Clicked Email, Opened Email}
    Any information that is unavailable will be `undefined`
    r_data : dict?, a received email dictionary with flow_id, message_id, variation, and count 
    c_data : dict?, a *metric* email dictionary with flow_id, message_id, variation, and counticant 
    */
  function compare_variations(
    apiKey,
    skeleton,
    flow_id,
    message_id,
    rID,
    cID,
    dateNow,
    timeFrame
  ) {
    let subDict = skeleton[flow_id]?.[message_id];
    if (subDict == undefined) return;
    let c_variations = Object.keys(subDict);

    let varID1 = c_variations[0];
    let varID2 = c_variations[1];

    const startDate = new Date(dateNow - timeFrame * 86400000)
      .toISOString()
      .slice(0, 10);
    const endDate = new Date(dateNow).toISOString().slice(0, 10);
    const countVarsReceivedDict = fetch_variation_counts(
      apiKey,
      rID,
      startDate,
      endDate
    );
    const countVarsQueryDict = fetch_variation_counts(
      apiKey,
      cID,
      startDate,
      endDate
    );

    let c_1 = countVarsQueryDict[varID1];
    let c_2 = countVarsQueryDict[varID2];

    let r_1 = countVarsReceivedDict[varID1];
    let r_2 = countVarsReceivedDict[varID2];

    let sig = significance(c_1, r_1, c_2, r_2);

    switch (sig) {
      // Siginificant: [metric_var1_count, received_var1_count, metric_var2_count, received_var2_count, winner, loser]
      case 1:
        return {
          metric1_count: c_1,
          received1_count: r_1,
          metric2_count: c_2,
          received2_count: r_2,
          winner: varID1,
          loser: varID2,
          variation1_id: varID1,
          variation2_id: varID2,
        };
      case 2:
        return {
          metric1_count: c_1,
          received1_count: r_1,
          metric2_count: c_2,
          received2_count: r_2,
          winner: varID2,
          loser: varID1,
          variation1_id: varID1,
          variation2_id: varID2,
        };

      // Insignificant: [metric_var1_count, received_var1_count, metric_var2_count, received_var2_count, winner, variation1, variation2]
      default:
        return {
          metric1_count: c_1,
          received1_count: r_1,
          metric2_count: c_2,
          received2_count: r_2,
          winner: undefined,
          variation1_id: varID1,
          variation2_id: varID2,
        };
    }
  }

  function dict_to_arr(data) {
    let result = [];
    let flow_arr = Object.keys(data);
    flow_arr.forEach(function (flow) {
      let message_arr = [];
      let messages = Object.keys(data[flow]);
      messages.forEach(function (message) {
        message_arr.push(message);
      });
      let obj = [flow, message_arr];
      result.push(obj);
    });
    return result;
  }

  // A class for test objects.
  // If there is no winner (test is insignificant, this.winner === undefined, and this.loser === [variation1_ID, va])
  class TestObject {
    constructor(
      flow_id,
      message_id,
      message_name,
      variation1_id,
      metric_1,
      received_1,
      variation2_id,
      metric_2,
      received_2,
      winner,
      loser
    ) {
      this.flow_id = flow_id;
      this.message_id = message_id;
      this.message_name = message_name;
      this.variation1_id = variation1_id;
      this.metric_1 = metric_1;
      this.received_1 = received_1;
      this.variation2_id = variation2_id;
      this.metric_2 = metric_2;
      this.received_2 = received_2;
      this.winner = winner;
      this.loser = loser;
    }
  }

  const find_intersection = (arr1, arr2) => {
    return {
      same: arr1.filter((x) => arr2.includes(x)),
      diff: arr1
        .filter((x) => !arr2.includes(x))
        .concat(arr2.filter((x) => !arr1.includes(x))),
    };
  };

  // Returns an array of form [result_sig_tests, result_insig_tests] that indicate which tests are insignificant and which tests are significant
  function get_significant_tests(
    apiKey,
    skeleton,
    rID,
    cID,
    dateNow,
    timeFrame
  ) {
    let message_id_dict = {};
    const arr_data = dict_to_arr(skeleton);
    let flowArr = [];

    for (const subArr of arr_data) {
      flowArr.push(subArr[0]);
    }

    console.log(arr_data);

    // const { same, diff } = find_intersection(result_c, result_r);
    let result_sig = [];
    let result_insig = [];
    for (let i = 0; i < arr_data.length; i++) {
      let flow = flowArr[i];
      let message_arr = Object.keys(skeleton[flow]);

      for (let j = 0; j < message_arr.length; j++) {
        let message = message_arr[j];
        let test = compare_variations(
          apiKey,
          skeleton,
          flow,
          message,
          rID,
          cID,
          dateNow,
          timeFrame
        );
        if (!test) continue;
        if (test.winner) {
          let message_name = message_id_dict[message];
          let test_obj = new TestObject(
            flow,
            message,
            message_name,
            test.variation1_id,
            test.metric1_count,
            test.received1_count,
            test.variation2_id,
            test.metric2_count,
            test.received2_count,
            test.winner,
            test.loser
          );
          result_sig.push(test_obj);
        } else {
          let message_name = message_id_dict[message];
          let test_obj = new TestObject(
            flow,
            message,
            message_name,
            test.variation1_id,
            test.metric1_count,
            test.received1_count,
            test.variation2_id,
            test.metric2_count,
            test.received2_count,
            test.winner,
            test.loser
          );
          result_insig.push(test_obj);
        }
      }
    }
    // return { data: [result_sig, result_insig, diff] };
    return { data: [result_sig, result_insig] };
  }

  function read_from_DDB(apiKey) {
    const awsConfig = {
      accessKeyId: "",
      secretAccessKey: "",
      region: "us-east-1",
    };

    AWS.config.update(awsConfig);
    const ddb = new AWS.DynamoDB.DocumentClient();

    const queryParams = {
      TableName: "SkeletonByAPIKeys",
      Key: {
        apiKeys: apiKey,
      },
    };

    const promise = ddb.get(queryParams).promise();

    return promise;
  }

  async function get_tests(apiKey, metric, timeFrame) {
    const dateNow = Date.now();

    let metric_name;
    if (metric == 0) {
      metric_name = "Opened Email";
    } else if (metric == 1) {
      metric_name = "Clicked Email";
    }

    let main_metricID = await extract_metrics_ID(apiKey, metric_name);
    let received_metricID = await extract_metrics_ID(apiKey, "Received Email");
    console.log(main_metricID, received_metricID);

    const tableEntry = await read_from_DDB(apiKey);
    console.log(tableEntry);
    const skeleton = tableEntry.Item.message;

    return get_significant_tests(
      apiKey,
      skeleton,
      received_metricID,
      main_metricID,
      dateNow,
      timeFrame
    );
  }

  try {
    return await res
      .status(200)
      .json(await get_tests(privateApiKey, metric, +daysAgo));
  } catch (error) {
    console.log(error);
  }
}
