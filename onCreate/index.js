const { S3 } = require('@aws-sdk/client-s3');
const sharp = require('sharp');
const mime = require('mime');
const ExifReader = require('exifreader');

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
        case 'ObjectCreated:Put':
            console.log('Handling put');
            await handlePut(bucketName, keyDecoded);
            break;
    }
};

const handlePut = async (bucketName, key) => {
    console.log(`Getting ${bucketName}/${key}`)
    const s3Object = await s3Client.getObject({ Bucket: bucketName, Key: key });
    const imageAsBuffer = await streamToBuffer(s3Object.Body);

    readExif(imageAsBuffer);

    const sharpImage = await sharp(imageAsBuffer);
    const metadata = await sharpImage.metadata();
    const promises = Object.keys(sizes)
        .map(size => generateSize(bucketName, key, size, sharpImage, metadata));
    await Promise.all(promises);
};

const readExif = (imageAsBuffer) => {
    const exif = ExifReader.load(imageAsBuffer);
    console.log(exif);
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
        .withMetadata()
        .toBuffer();

    const params = {
        Bucket: bucketName,
        Key: fileNameToSize(key, size),
        Body: resizedImage,
        ContentType: mime.getType(key)
    };

    console.log(`Saving ${params.Key}`);

    return s3Client.putObject(params);
}

const streamToBuffer = (stream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};

const fileNameToSize = (key, size) => {
    const [albumName, _, fileName] = key.split('/');
    return `${albumName}/${size}/${fileName}`;
};