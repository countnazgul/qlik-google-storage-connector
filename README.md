# Qlik (View and Sense) Google Cloud Storage Custom Connector

#### Description

Custom connector that allows interaction with `Google Cloud Storage` via Qlik Script

#### Instalation

Copy the files to `C:\Program Files\Common Files\QlikTech\Custom Data\QvEventLogConnectorElaborate`. 
If the path do not exist - make sure to create the folders first.

#### Usage

*CredentialFile* should be provided for (almost) **all** methods!

#### Methods

##### ListBuckets
  * Description - list all available buckets for specific project
  * Returned fields
    * Acl
    * Billing
    * Cors
    * ETag
    * Encryption
    * Id
    * Kind
    * Label
    * Lifecycle
    * Location
    * Logging
    * Name
    * Owner
  * Required params
    * *CredentialFile*
    * *ProjectId* - GCP project ID

##### ListObjects
  * Description - list all available objects for a specific buckets name
  * Returned fields

  * Required params
    * *CredentialFile*
    * *BucketName*

##### DownloadObject
  * Description - download file to local location
  * Returned fields

  * Required params
    * *CredentialFile*
    * *BucketName* 
    * *ObjectName*
    * *LocalPath*

##### UploadObject
  * Description - upload local file to bucket
  * Returned fields

  * Required params
    * *CredentialFile*
    * *LocalPath*
    * *BucketName* 
    * *???ObjectName???*

##### DeleteLocalObject
  * Description - delete local file
  * Returned fields

  * Required params
    * *LocalPath*