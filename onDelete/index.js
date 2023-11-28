const { S3 } = require('@aws-sdk/client-s3');
const sharp = require('sharp');
const mime = require('mime');

const s3Client = new S3();

const sizes = {
    large: 1920,
    medium: 1024,
    small: 512,
    thumb: 256
};

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

    const folderName = key.split('/')[1];
    if (folderName !== 'full') {
        console.log(`Not master folder: ${key} - exiting`);
        return;
    }

    if(!mime.getType(key).includes('image')) {
        console.log(`Not an image: ${key} - exiting`);
        return;
    }

    const keyDecoded = decodeURIComponent(key.replace(/\+/g, ' '));

    switch (eventName) {
        case 'ObjectRemoved:Delete':
            console.log('Handling delete');
            await handleDelete(bucketName, keyDecoded);
            break;
    }
};

const handleDelete = async (bucketName, key) => {
    const promises = Object.keys(sizes)
        .map(size => {
            const keyToDelete = fileNameToSize(key, size);
            console.log(`Deleting ${bucketName}/${keyToDelete}`);
            return s3Client.deleteObject({ Bucket: bucketName, Key: keyToDelete });
        });
    await Promise.all(promises);
};