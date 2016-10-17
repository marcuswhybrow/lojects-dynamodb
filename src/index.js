import AWS from 'aws-sdk';
import { Integration } from 'lojects';
import { AttributeValue as attr } from 'dynamodb-data-types';
import { flatten } from './utils';

const dynamodb = new AWS.DynamoDB();

export default new Integration({
  name: 'DynamoDB',
  defaultContext: { idAttrName: 'id' },
  actions: {
    create: (data, context) => {
      return dynamodb.putItem({
        Item: attr.wrap(data),
        TableName: context.tableName,
        ConditionExpression: `attribute_not_exists (${context.idAttrName})`
      }).promise().then(() => data)
    },

    get: (data, context) => {
      let key = {};
      key[context.idAttrName] = data[context.idAttrName];
      return dynamodb.getItem({
        Key: attr.wrap(key),
        TableName: context.tableName
      }).promise().then(data => {
        if (!data.hasOwnProperty('Item'))
          return Promise.reject("Item not found");
        return attr.unwrap(data.Item)
      });
    },

    update: (data, context) => {
      // Extract id from data to use as the update key
      let key = {};
      key[context.idAttrName] = attr.wrap(data[context.idAttrName]);
      delete data[context.idAttrName];

      // build update expression & attribute values
      let updateExpression = '';
      let attrValues = {};
      Object.keys(flatten(data)).forEach((nestedAttrName, index) => {
        updateExpression += `SET ${nestedAttrName} = :val${index}, `;
        attrValues[`:val${index}`] = attr.wrap(data[nestedAttrName]);
      });

      // Remove trailing comma
      const expLen = updateExpression.length;
      if (expLen >= 2) {
        updateExpression = updateExpression.substring(0, expLen - 2);
      }

      // call dynamodb
      return dynamodb.getItem({
        Key: attr.wrap(key),
        UpdateExpression: updateExpression,
        ExpressionAttributeValues: attrValues,
        TableName: context.tableName,
        ReturnValues: 'ALL_NEW',
        ConditionExpression: `attribute_exists (${context.idAttrName})`
      }).promise().then(data => attr.unwrap(data.Item));
    }
  },
});
