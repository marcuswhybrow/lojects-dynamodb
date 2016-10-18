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
      const id = data[context.idAttrName];
      console.info(`${context.manager.name}: CREATE ${id} in ${context.tableName}`);
      return dynamodb.putItem({
        Item: attr.wrap(data),
        TableName: context.tableName,
        ConditionExpression: `attribute_not_exists (${context.idAttrName})`
      }).promise()
        .then(() => {
          console.info(`${context.manager.name}: CREATE successfull`);
          return data;
        })
        .catch(err => {
          console.error(err);
          return Promise.reject(err.message);
        });
    },

    get: (data, context) => {
      let key = {};
      const id = data[context.idAttrName];
      key[context.idAttrName] = id;
      console.info(`${context.manager.name}: GET ${id} from ${context.tableName}`)
      return dynamodb.getItem({
        Key: attr.wrap(key),
        TableName: context.tableName
      }).promise().then(data => {
        if (!data.hasOwnProperty('Item')) {
          console.error("Item not found");
          return Promise.reject("Item not found");
        }
        console.info(`${context.manager.name}: GET successfull`);
        return attr.unwrap(data.Item)
      });
    },

    update: (data, context) => {
      const id = data[context.idAttrName];
      let key = {};
      let updateExpression = 'SET';
      let attrNames = {};
      let attrValues = {};

      const extractKey = () => {
        key[context.idAttrName] = id;
        delete data[context.idAttrName];
        return Promise.resolve(key);
      };
      const buildUpdateExpression = () => {
        const flatData = flatten(data);
        const buildAttrNames = (nestedAttrName) => {
          nestedAttrName.split('.').forEach(attrName => {
            attrNames[`#${attrName}`] = attrName;
          });
        };
        const buildAttrValues = (nestedAttrName, index) => {
          const attrName = '#' + nestedAttrName.replace(/\./g, '.#');
          updateExpression += ` ${attrName} = :val${index}, `;
          attrValues[`:val${index}`] = flatData[nestedAttrName];
        };
        const removeTrailingComma = () => {
          let expLen = updateExpression.length;
          if (expLen >= 2) {
            updateExpression = updateExpression.substring(0, expLen - 2);
          }
        };

        Object.keys(flatData).forEach((nestedAttrName, index) => {
          buildAttrNames(nestedAttrName);
          buildAttrValues(nestedAttrName, index);
        });
        removeTrailingComma();
      };
      const callIntegration = () => {
        console.info(`${context.manager.name}: UPDATE ${id} in ${context.tableName}`);
        return dynamodb.updateItem({
          Key: attr.wrap(key),
          UpdateExpression: updateExpression,
          ExpressionAttributeNames: attrNames,
          ExpressionAttributeValues: attr.wrap(attrValues),
          TableName: context.tableName,
          ReturnValues: 'ALL_NEW',
          ConditionExpression: `attribute_exists (${context.idAttrName})`
        }).promise().then(data => {
          console.info(`${context.manager.name}: UPDATE successfull`);
          return attr.unwrap(data.Attributes);
        }).catch(error => {
          console.error(error);
          return Promise.reject(error.message);
        });
      };

      return extractKey()
        .then(buildUpdateExpression)
        .then(callIntegration);
    }
  },
});
