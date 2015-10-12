When running spark submit, use the following jar command line arguments
--key : AWS Key
--secret_key : AWS Secret Key
--input : Directory for file pickup.  if location = aws, then we'd use the existing staging structure,
else choose a local directory
--output : Directory for post process drop.  Same rules for input, note that this directory cannot exist for spark
to succeed.
--location : Location setting.  This impacts the behavior of the job. Location options are:  [aws,local]


sample spark-submit job:

sudo ./spark-submit --class org.merkle.mde.PixelLogProcessorApp .../pixellogprocessor_2.10-1.0.jar --input s3n://... --output s3n://... --key xxxx --secret_key xxxx --location aws
