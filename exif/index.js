const { S3 } = require('@aws-sdk/client-s3');
const mime = require('mime');
const ExifReader = require('exifreader');

const s3Client = new S3();

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    const {
        eventName,
        s3: {
            bucket : {
                name: bucketName
            },
            object: {
                key
            }
        }
    } = event.Records[0];

    if(!mime.getType(key).includes('image')) {
        console.log(`Not an image: ${key} - exiting`);
        return;
    }

    const keyDecoded = decodeURIComponent(key.replace(/\+/g, ' '));

    switch (eventName) {
        case 'ObjectCreated:Put':
            console.log('Handling put');
            await handlePut(bucketName, keyDecoded);
            break;
    }
};

const handlePut = async (bucketName, key) => {
    console.log(`Getting ${bucketName}/{key}`)
    const s3Object = await s3Client.getObject({ Bucket: bucketName, Key: key });
    const imageAsBuffer = await streamToBuffer(s3Object.Body);
    const exif = ExifReader.load(imageAsBuffer);
    console.log(exif);
};

const streamToBuffer = (stream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};