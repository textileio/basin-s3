use adm_sdk::machine;
use adm_sdk::machine::objectstore::AddOptions;
use adm_sdk::machine::objectstore::DeleteOptions;
use adm_sdk::machine::objectstore::QueryOptions;
use async_tempfile::TempFile;
use bytestring::ByteString;
use fendermint_vm_message::query::FvmQueryHeight;
use futures::StreamExt;
use futures::TryStreamExt;
use md5::Md5;
use s3s::S3Error;
use s3s::S3ErrorCode;
use std::ops::Not;
use tendermint_rpc::Client;
use tokio::fs;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use adm_sdk::machine::objectstore::GetOptions;
use adm_sdk::machine::objectstore::ObjectStore;
use adm_sdk::machine::Machine;
use fendermint_actor_machine::WriteAccess;
use fendermint_actor_objectstore::ObjectListItem;
use fvm_shared::address::Address;
use md5::Digest;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use std::str::FromStr;
use uuid::Uuid;

use crate::utils::bytes_stream;
use crate::utils::copy_bytes;
use crate::utils::hex;
use crate::Basin;

#[async_trait::async_trait]
impl<C> S3 for Basin<C>
where
    C: Client + Send + Sync + 'static,
{
    // #[tracing::instrument]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "AbortMultipartUpload is not implemented in read-only mode"
            ));
        }
        let AbortMultipartUploadInput { upload_id, .. } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        let prefix = format!(".upload_id-{upload_id}");
        let mut iter = try_!(fs::read_dir(&self.root).await);
        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            if name.starts_with(&prefix) {
                try_!(fs::remove_file(entry.path()).await);
            }
        }
        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CompleteMultipartUpload is not implemented in read-only mode"
            ));
        }

        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let mut file = try_!(TempFile::new().await);

        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part
                .part_number
                .ok_or_else(|| s3_error!(InvalidRequest, "missing part number"))?;
            cnt += 1;
            if part_number != cnt {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }

            let part_path = self.get_upload_part_path(&upload_id, part_number);
            let mut reader = try_!(fs::File::open(&part_path).await);
            let _ = try_!(tokio::io::copy(&mut reader, &mut file).await);

            try_!(fs::remove_file(&part_path).await);
        }

        try_!(file.flush().await);
        try_!(file.rewind().await);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let address = try_!(Address::from_str(bucket.as_str()));
        let machine = ObjectStore::attach(address);

        let tx = machine
            .add(
                &self.provider,
                &mut wallet,
                &key,
                file,
                AddOptions::default(),
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            //e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                ..
            } => (bucket.to_string(), key.to_string()),
        };

        let (dst_bucket, dst_key) = (input.bucket, input.key);

        // Download object to a file
        let address = try_!(Address::from_str(&src_bucket));
        let machine = ObjectStore::attach(address);

        let mut file = try_!(TempFile::new().await);
        let (writer, mut reader) = tokio::io::duplex(4096);

        let () = machine
            .get(
                &self.provider,
                src_key.as_str(),
                writer,
                GetOptions {
                    range: None,
                    height: FvmQueryHeight::Committed,
                    show_progress: false,
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        try_!(tokio::io::copy(&mut reader, &mut file).await);

        // Upload file
        try_!(file.flush().await);
        try_!(file.rewind().await);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let address = try_!(Address::from_str(&dst_bucket));
        let machine = ObjectStore::attach(address);

        let _ = machine
            .add(
                &self.provider,
                &mut wallet,
                &dst_key,
                file,
                AddOptions::default(),
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let copy_object_result = CopyObjectResult {
            //e_tag: Some(format!("\"{md5_sum}\"")),
            last_modified: Timestamp::parse(TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z")
                .ok(),
            ..Default::default()
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CreateBucket is not implemented in read-only mode"
            ));
        }

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let (machine, _) = ObjectStore::new(
            &self.provider,
            &mut wallet,
            WriteAccess::OnlyOwner,
            Default::default(),
        )
        .await
        .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let address = machine.address().to_string();

        Ok(S3Response::new(CreateBucketOutput {
            location: Some(address),
        }))
    }

    // #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CreateMultipartUpload is not implemented in read-only mode"
            ));
        }

        let input = req.input;
        let upload_id = Uuid::new_v4();

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObject is not implemented in read-only mode"
            ));
        }

        let bucket = req.input.bucket;
        let key = req.input.key;

        let address = try_!(Address::from_str(&bucket));
        let machine = ObjectStore::attach(address);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let tx = machine
            .delete(
                &self.provider,
                &mut wallet,
                key.as_str(),
                DeleteOptions::default(),
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        debug!(hash = ?tx.hash, status = ?tx.status);

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObjects is not implemented in read-only mode"
            ));
        }

        let address = try_!(Address::from_str(&req.input.bucket));
        let machine = ObjectStore::attach(address);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        for object in req.input.delete.objects {
            let tx = machine
                .delete(
                    &self.provider,
                    &mut wallet,
                    object.key.as_str(),
                    DeleteOptions::default(),
                )
                .await
                .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

            debug!(hash = ?tx.hash, status = ?tx.status);
        }

        let output = DeleteObjectsOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address);

        let object_list = machine
            .query(
                &self.provider,
                QueryOptions {
                    prefix: input.key.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object_item = if let Some(object) = object_list.objects.into_iter().next() {
            object.1
        } else {
            return Err(s3_error!(NoSuchKey));
        };

        let file_len = match object_item {
            ObjectListItem::Internal((_, size)) => size,
            _ => {
                unimplemented!("external object")
            }
        };
        let (content_length, content_range) = match input.range {
            None => (file_len, None),
            Some(range) => {
                let file_range = range.check(file_len)?;
                let content_length = file_range.end - file_range.start;
                let content_range =
                    fmt_content_range(file_range.start, file_range.end - 1, file_len);
                (content_length, Some(content_range))
            }
        };
        let content_length_usize = try_!(usize::try_from(content_length));
        let content_length_i64 = try_!(i64::try_from(content_length));

        let range = match input.range {
            Some(Range::Int { first, last }) => Some(format!(
                "{}-{}",
                first,
                last.map_or(String::new(), |v| v.to_string())
            )),
            Some(Range::Suffix { length }) => Some(format!("-{length}")),
            _ => None,
        };

        let (writer, reader) = tokio::io::duplex(4096);
        let reader_stream = tokio_util::io::ReaderStream::new(reader);

        let body = bytes_stream(reader_stream, content_length_usize);

        let () = machine
            .get(
                &self.provider,
                input.key.as_str(),
                writer,
                GetOptions {
                    range,
                    height: FvmQueryHeight::Committed,
                    show_progress: false,
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(body)),
            content_length: Some(content_length_i64),
            content_range,
            last_modified: Timestamp::parse(TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z")
                .ok(),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let _ = machine::info(&self.provider, address, FvmQueryHeight::Committed)
            .await
            .map_err(|_| s3_error!(NoSuchBucket))?;

        Ok(S3Response::new(HeadBucketOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address);

        let object_list = machine
            .query(
                &self.provider,
                QueryOptions {
                    prefix: input.key.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object_item = if let Some(object) = object_list.objects.into_iter().next() {
            object.1
        } else {
            return Err(s3_error!(NoSuchKey));
        };

        let content_length_u64 = match object_item {
            ObjectListItem::Internal((_, size)) => size,
            _ => {
                unimplemented!("external object")
            }
        };
        let content_length_i64 = try_!(i64::try_from(content_length_u64));

        // TODO: detect content type
        let content_type = mime::APPLICATION_OCTET_STREAM;

        let output = HeadObjectOutput {
            content_length: Some(content_length_i64),
            content_type: Some(content_type),
            last_modified: Timestamp::parse(TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z")
                .ok(),
            metadata: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "ListBuckets is not implemented in read-only mode"
            ));
        }

        let wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let list = ObjectStore::list(&self.provider, &wallet, FvmQueryHeight::Committed)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut buckets: Vec<Bucket> = Vec::new();

        for data in list {
            let bucket = Bucket {
                //creation_date: Some(created_or_modified_date),
                creation_date: None,
                name: Some(data.address.to_string()),
            };
            buckets.push(bucket);
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input: ListObjectsV2Input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address);

        let mut objects: Vec<Object> = Vec::new();

        let prefix = input.prefix.clone().unwrap();
        let response = machine
            .query(
                &self.provider,
                QueryOptions {
                    prefix,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        for (key, item) in response.objects {
            let key_str = try_!(String::from_utf8(key));

            let object = match item {
                ObjectListItem::Internal((_, size)) => Object {
                    key: Some(key_str),
                    //last_modified: Some(last_modified),
                    size: Some(try_!(i64::try_from(size))),
                    ..Default::default()
                },
                ObjectListItem::External((_, _)) => {
                    Object {
                        key: Some(key_str),
                        //last_modified: Some(last_modified),
                        //size: Some(try_!(i64::try_from(size))),
                        ..Default::default()
                    }
                }
            };
            objects.push(object);
        }

        let key_count = try_!(i32::try_from(objects.len()));

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            delimiter: input.delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            prefix: input.prefix,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "PutObject is not implemented in read-only mode"
            ));
        }
        let input = req.input;

        let PutObjectInput {
            body, bucket, key, ..
        } = input;

        let Some(mut body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let address = try_!(Address::from_str(bucket.as_str()));
        let machine = ObjectStore::attach(address);

        let mut file = try_!(TempFile::new().await);

        while let Some(Ok(v)) = body.next().await {
            try_!(file.write_all(&v).await);
        }
        try_!(file.flush().await);
        try_!(file.rewind().await);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let tx = machine
            .add(
                &self.provider,
                &mut wallet,
                &key,
                file,
                AddOptions::default(),
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let output = PutObjectOutput {
            //e_tag: Some(e_tag),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "UploadPart is not implemented in read-only mode"
            ));
        }

        let UploadPartInput {
            body,
            upload_id,
            part_number,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let file_path = self.get_upload_part_path(&upload_id, part_number);
        let mut md5_hash = <Md5 as Digest>::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut file = try_!(fs::File::create_new(&file_path).await);
        let size = copy_bytes(stream, &mut file).await?;
        try_!(file.flush().await);

        let md5_sum = hex(md5_hash.finalize());
        debug!(path = ?file_path, ?size, %md5_sum, "write file");

        let output = UploadPartOutput {
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_bucket_location(
        &self,
        _req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }
}

/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}
