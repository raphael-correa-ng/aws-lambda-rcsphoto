const { SecretsManager } = require('@aws-sdk/client-secrets-manager');
const NodeCouchDb = require('node-couchdb');

const secretId = '/amplify/d3pxpc2g28m074/master/cloudant_credentials';
const region = 'us-west-2';

const client = new SecretsManager({ region });

exports.handler = async (event) => {
  const secret = await client.getSecretValue({ SecretId: secretId });

  const {
    host,
    port,
    username,
    password
  } = JSON.parse(secret.SecretString);

  const dbClient = new NodeCouchDb({
     protocol: 'https',
     host,
     port,
     auth: { user: username, pass: password }
   });

  const response = await dbClient.get('albums', '_all_docs?include_docs=true');
  
  return {
    statusCode: 200,
    headers: {
      'Access-Control-Expose-Headers': 'Access-Control-Allow-Origin',
      'Access-Control-Allow-Credentials': true,
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    },
    body: response,
  };
};
