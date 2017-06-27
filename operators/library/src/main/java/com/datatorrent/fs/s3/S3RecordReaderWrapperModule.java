package com.datatorrent.fs.s3;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.fs.FSRecordReader;
import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.fs.S3BlockReader;
import com.datatorrent.lib.metrics.S3RecordReader;

/**
 * Wrapper for S3RecordReaderModule and specify the below parameters as configuration to this module:
 *  - accessKey : AWS key to access S3 bucket
 *  - secretKey : AWS secret key to access S3 bucket
 *  - bucketName  : Name of S3 bucket
 *  - s3Schema  : Schema of S3 File System.
 */
public class S3RecordReaderWrapperModule extends FSRecordReaderModule
{
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @NotNull
  private String s3Schema = "s3n";
  /**
   * Endpoint for S3
   */
  private String s3EndPoint;
  @Min(0)
  private int overflowBlockSize;

  /**
   * Creates an instance of Record Reader
   *
   * @return S3RecordReader instance
   */
  @Override
  public FSRecordReader createRecordReader()
  {
    S3RecordReader s3RecordReader = new S3RecordReader();
    s3RecordReader.setBucketName(S3BlockReader.extractBucket(getFiles()));
    s3RecordReader.setAccessKey(S3BlockReader.extractAccessKey(getFiles()));
    s3RecordReader.setSecretAccessKey(S3BlockReader.extractSecretAccessKey(getFiles()));
    s3RecordReader.setEndPoint(s3EndPoint);
    s3RecordReader.setMode(this.getMode());
    s3RecordReader.setRecordLength(this.getRecordLength());
    if (overflowBlockSize != 0) {
      s3RecordReader.setOverflowBufferSize(overflowBlockSize);
    }
    return s3RecordReader;
  }


  /**
   * Convert the the input files to S3URI and set it to files.
   */
  private void generateAndSetS3URIToInput()
  {
    String inputURI = s3Schema + "://" + accessKey + ":" + secretKey + "@" + bucketName + "/";
    String uriFiles = "";
    String[] inputFiles = super.getFiles().split(",");
    for (int i = 0; i < inputFiles.length; i++) {
      uriFiles += inputURI + inputFiles[i];
      if (i != inputFiles.length - 1) {
        uriFiles += ",";
      }
    }
    this.setFiles(uriFiles);
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    generateAndSetS3URIToInput();
    super.populateDAG(dag, configuration);
  }

  /**
   * Set the S3 endpoint to use
   *
   * @param s3EndPoint
   */
  public void setS3EndPoint(String s3EndPoint)
  {
    this.s3EndPoint = s3EndPoint;
  }

  /**
   * Returns the s3 endpoint
   *
   * @return s3EndPoint
   */
  public String getS3EndPoint()
  {
    return s3EndPoint;
  }

  /**
   * additional data that needs to be read to find the delimiter character for
   * last record in a block. This should be set to approximate record size in
   * the file, default value 1MB
   *
   * @param overflowBlockSize
   */
  public void setOverflowBlockSize(int overflowBlockSize)
  {
    this.overflowBlockSize = overflowBlockSize;
  }

  /**
   * returns the overflow block size
   *
   * @return overflowBlockSize
   */
  public int getOverflowBlockSize()
  {
    return overflowBlockSize;
  }

  /**
   * Return the S3 access key
   * @return accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the S3 access key
   * @param accessKey accessKey
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Return the S3 secret Key
   * @return secretKey
   */
  public String getSecretKey()
  {
    return secretKey;
  }

  /**
   * Set the S3 secret Key
   * @param secretKey secretKey
   */
  public void setSecretKey(String secretKey)
  {
    this.secretKey = Preconditions.checkNotNull(secretKey);
  }

  /**
   * Return the bucket name
   * @return bucketName
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name
   * @param bucketName bucketName
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Get the schema for s3 file system
   * @return s3Schema
   */
  public String getS3Schema()
  {
    return s3Schema;
  }

  /**
   * Set the schema for s3 file system
   * @param s3Schema s3Schema
   */
  public void setS3Schema(String s3Schema)
  {
    this.s3Schema = Preconditions.checkNotNull(s3Schema);
  }
}
