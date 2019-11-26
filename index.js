const aws = require("aws-sdk");
const s3 = new aws.S3();
const zlib = require("zlib");

exports.handler = (event, context, callback) => {

  var bucketName = event.Records[0].s3.bucket.name;
  var uploadFilename = event.Records[0].s3.object.key;
  var gunzipFileName = uploadFilename.replace('.gz', '.log');

  const uploadParams = {
    Bucket: bucketName,
    Key: uploadFilename
  };

  s3.getObject(uploadParams, (err, getData) => {
    if (err) {
      console.log("getObject: " + uploadFilename);
      console.log(err, err.stack);
      callback(err);
    }
    else {
      console.log("Start: " + uploadFilename);
      const body = getData.Body;
      zlib.gunzip(body, (err, unzipData) => {
        if (err) {
          console.log("zlib.gunzip: " + uploadFilename);
          console.log(err, err.stack);
          callback(err);
        }
        else {
          var createMultipartUploadParams = {
            Bucket: bucketName,
            Key: gunzipFileName,
            StorageClass: "REDUCED_REDUNDANCY"
          };

          s3.createMultipartUpload(createMultipartUploadParams, function(err, uploadData) {
            if (err) {
              console.log("createMultipartUpload: " + uploadFilename);
              console.log(err, err.stack);
              callback(err);
            }

            var uploadPartParams = {
              Bucket: bucketName,
              Key: gunzipFileName,
              Body: unzipData,
              PartNumber: 1,
              UploadId: uploadData.UploadId
            };

            s3.uploadPart(uploadPartParams, function(err, uploadPartData) {
              if (err) {
                console.log("uploadPart: " + uploadFilename);
                console.log(err, err.stack);
                callback(err);
              };

              var completeMultipartUploadParams = {
                Bucket: bucketName,
                Key: gunzipFileName,
                MultipartUpload: {
                  Parts: [{
                    ETag: uploadPartData.ETag,
                    PartNumber: 1
                  }]
                },
                UploadId: uploadData.UploadId
              };

              s3.completeMultipartUpload(completeMultipartUploadParams, function(err, completeMultipartUploadData) {
                if (err) {
                  console.log("completeMultipartUpload: " + uploadFilename);
                  console.log(err, err.stack);
                  callback(err);
                }
                // else {
                //   console.log(completeMultipartUploadData);
                // }

                s3.deleteObject(uploadParams, function(err, data) {
                  if (err)
                    console.log(err, err.stack);
                  else
                    console.log("End: " + uploadFilename);
                });
              });
            });
          });
        }
      });
    }
  });
};
