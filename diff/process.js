"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
const util = require("util");
const ddb = new AWS.DynamoDB.DocumentClient();
const TableName = process.env.DYNAMODB_TABLE_NAME || '';
const timezone = 8; //目标时区时间，东八区
var offset_GMT = new Date().getTimezoneOffset(); // 本地时间和格林威治的时间差，单位为分钟
//年~分钟
function dimensions0(timestamp) {
    const date = new Date(timestamp);
    const metricList = ['YEAR', 'MONTH', 'DATE', 'HOUR', 'MINUTE'];
    const split = {
        Date: date.getUTCDate() >= 10 ? date.getUTCDate() : ('0' + date.getUTCDate()),
        Hour: date.getUTCHours() >= 10 ? date.getUTCHours() : ('0' + date.getUTCHours()),
        Minute: date.getUTCMinutes() >= 10 ? date.getUTCMinutes() : ('0' + date.getUTCMinutes()),
        Month: date.getUTCMonth() >= 10 ? (date.getUTCMonth() + 1) : ('0' + (date.getUTCMonth() + 1)),
        Year: date.getUTCFullYear()
    };
    const dateString = util.format('%s-%s-%sT%s:%s', split.Year, split.Month, split.Date, split.Hour, split.Minute);
    const dateList = [];
    while (metricList.length > 0) {
        const metric = metricList.pop();
        dateList.push(util.format('%s:%s', metric, dateString.substr(0, 3 * metricList.length + 4)));
    }
    return dateList;
}
//千年~分钟
function dimensions1(timestamp) {
    const date = new Date(timestamp);
    const metricList = ['MILLENNIUM', 'YEAR', 'MONTH', 'DATE', 'HOUR', 'MINUTE'];
    const split = {
        Date: date.getUTCDate() >= 10 ? date.getUTCDate() : ('0' + date.getUTCDate()),
        Hour: date.getUTCHours() >= 10 ? date.getUTCHours() : ('0' + date.getUTCHours()),
        Minute: date.getUTCMinutes() >= 10 ? date.getUTCMinutes() : ('0' + date.getUTCMinutes()),
        Month: date.getUTCMonth() >= 10 ? (date.getUTCMonth() + 1) : ('0' + (date.getUTCMonth() + 1)),
        Year: date.getUTCFullYear(),
        All: 'ALL'
    };
    const dateString = util.format('%s-%s-%sT%s:%s', split.Year, split.Month, split.Date, split.Hour, split.Minute);
    const dateList = [];
    while (metricList.length > 0) {
        const metric = metricList.pop();
//        dateList.push(util.format('%s:%s', metric, dateString.substr(0, 3 * metricList.length + 4)));
        dateList.push(util.format('%s:%s', metric, dateString.substr(0, 3 * metricList.length + 1)));
    }
    return dateList;
}
//千年~小时
function dimensions(timestamp) {
  const date = new Date(timestamp);
  const metricList = ['MILLENNIUM', 'YEAR', 'MONTH', 'DATE', 'HOUR'];
  const split = {
      Date: date.getUTCDate() >= 10 ? date.getUTCDate() : ('0' + date.getUTCDate()),
      Hour: date.getUTCHours() >= 10 ? date.getUTCHours() : ('0' + date.getUTCHours()),
      Minute: date.getUTCMinutes() >= 10 ? date.getUTCMinutes() : ('0' + date.getUTCMinutes()),
      Month: date.getUTCMonth() >= 10 ? (date.getUTCMonth() + 1) : ('0' + (date.getUTCMonth() + 1)),
      Year: date.getUTCFullYear(),
      All: 'ALL'
  };
  const dateString = util.format('%s-%s-%sT%s', split.Year, split.Month, split.Date, split.Hour);
  const dateList = [];
  while (metricList.length > 0) {
      const metric = metricList.pop();
//      dateList.push(util.format('%s:%s', metric, dateString.substr(0, 3 * metricList.length + 4)));
      dateList.push(util.format('%s:%s', metric, dateString.substr(0, 3 * metricList.length + 1)));
  }
  return dateList;
}
// function update(data) {
//     console.log(data);
//     const name = data.name;
//     const id = util.format('%s:%s', data.website, data.url);
//     if (!data || !data.url) {
//         return Promise.resolve();
//     }
//     return Promise.all(dimensions(data.date).map((date) => ddb.update({
//         ExpressionAttributeNames: { '#value': 'value', '#name': 'name' },
//         ExpressionAttributeValues: { ':inc': 1, ':name': name },
//         Key: { id, date },
//         TableName,
//         UpdateExpression: 'ADD #value :inc SET #name = :name'
//     }).promise()));
// }
function update(data) {
    console.log(data);
    const name = data.name;
    const id = util.format('%s:%s', data.website, data.url);
    if (!data || !data.url) {
        return Promise.resolve();
    }
    return Promise.all(dimensions(data.date).map((date) => ddb.update({
        ExpressionAttributeNames: { '#value': 'value', '#name': 'name' },
        ExpressionAttributeValues: { ':inc': 1, ':name': name },
        Key: { id, date },
        TableName,
        UpdateExpression: 'ADD #value :inc SET #name = :name'
    }).promise()));
}
function updateAll(data) {
    console.log(data);
    const name = data.name;
    const id = util.format('%s:%s', 'ALL', data.website);
    if (!data || !data.url) {
        return Promise.resolve();
    }
    return Promise.all(dimensions(data.date).map((date) => ddb.update({
        ExpressionAttributeNames: { '#value': 'value', '#name': 'name' },
        ExpressionAttributeValues: { ':inc': 1, ':name': name },
        Key: { id, date },
        TableName,
        UpdateExpression: 'ADD #value :inc SET #name = :name'
    }).promise()));
}
function run(event, context, callback) {
    const data = event.Records || [];
    // Create Promise for every received event
    const list = data.map((item) => {
        const buff = new Buffer(item.kinesis.data, 'base64').toString('ascii');
        try {
            // Update valid data
            // return update(JSON.parse(buff));
            let json = JSON.parse(buff);
            json.date = new Date().getTime() + offset_GMT * 60 * 1000 + timezone * 60 * 60 * 1000;
            let one = update(json);
            let all = updateAll(json);
            return Promise.all([one, all]);
        }
        catch (error) {
            // Ingore invalid data
            console.log(error);
            return Promise.resolve();
        }
    });
    // Wait until all Promises resolve and execute callback
    Promise.all(list).then(() => callback(null, { done: true, num: list.length }));
}
exports.run = run;
//# sourceMappingURL=process.js.map