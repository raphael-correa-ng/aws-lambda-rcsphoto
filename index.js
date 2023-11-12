const { S3 } = require('@aws-sdk/client-s3');
const sharp = require('sharp');

const s3Client = new S3();

const sizes = {
//    large: 1920,
//    medium: 1024,
//    small: 512,
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
        return;
    }

    switch (eventName) {
        case 'ObjectCreated:Put':
            await handlePut(bucketName, key);
            break;
        case 'ObjectRemoved:Delete':
            // todo: create a separate lambda for the clean up
            await handleDelete(bucketName, key);
            break;
    }

};

const handlePut = async (bucketName, key) => {
    console.log('Handling put');
    const s3Object = await tryGetS3Object(bucketName, key);
    // todo: assert content type
    await generateSizes(bucketName, key, s3Object);
};

const handleDelete = async (bucketName, key) => {
    console.log('Handling delete');
    for (const size of Object.keys(sizes)) {
        await tryDeleteS3Object(bucketName, fileNameToSize(key, size));
    }
};


const generateSizes = async (bucketName, key, s3Object) => {
    const imageAsBuffer = await streamToBuffer(s3Object.Body)
    const sharpImage = await sharp(imageAsBuffer);
    const metadata = await sharpImage.metadata();
    const promises = Object.keys(sizes)
        .map(size => generateSize(bucketName, key, size, sharpImage, metadata));
    await Promise.all(promises);
}

const generateSize = async (bucketName, key, size, sharpImage, metadata) => {
    const { width, height } = metadata;
    const maxSize = sizes[size];

    let newWidth, newHeight;
    if (width > height) {
        newWidth = maxSize;
        newHeight = Math.floor(height * maxSize / width);
    } else {
        newHeight = maxSize;
        newWidth = Math.floor(width * maxSize / height);
    }

    const resizedImage = await sharpImage
        .resize({ width: newWidth, height: newHeight })
        .jpeg({ quality: 100 })
        .withMetadata()
        .toBuffer();

    const params = {
        Bucket: bucketName,
        Key: fileNameToSize(key, size),
        Body: resizedImage,
        ContentType: 'image/jpeg'
    };
    console.log(`Saving ${params.Key}`);
    return s3Client.putObject(params);
}

const streamToBuffer = (stream) =>
    new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });

const fileNameToSize = (key, size) => {
    const [albumName, _, fileName] = key.split('/');
    return `${albumName}/${size}/${fileName}`;
}

const tryGetS3Object = async (bucketName, key) => {
    const params = {
        Bucket: bucketName,
        Key: decodeURIComponent(key.replace(/\+/g, ' ')),
    };
    try {
        return await s3Client.getObject(params);
    } catch (err) {
        console.log(err);
        throw err;
    }
};

const tryDeleteS3Object = async (bucketName, key) => {
    const params = {
        Bucket: bucketName,
        Key: decodeURIComponent(key.replace(/\+/g, ' '))
    };
    try {
        console.log(`Deleting ${bucketName}/${key}`);
        await s3Client.deleteObject(params);
    } catch (err) {
        console.log(err);
        throw err;
    }
};
