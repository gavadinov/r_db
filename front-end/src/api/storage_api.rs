#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutRequest {
    #[prost(int64, tag = "1")]
    pub shard_id: i64,
    #[prost(string, tag = "2")]
    pub key: std::string::String,
    #[prost(string, tag = "3")]
    pub val: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRequest {
    #[prost(int64, tag = "1")]
    pub shard_id: i64,
    #[prost(string, tag = "2")]
    pub key: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(int64, tag = "1")]
    pub shard_id: i64,
    #[prost(string, tag = "2")]
    pub key: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(string, tag = "1")]
    pub val: std::string::String,
}
#[doc = r" Generated server implementations."]
pub mod storage_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct StorageClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl StorageClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> StorageClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetRequest>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/storage_api.Storage/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn put(
            &mut self,
            request: impl tonic::IntoRequest<super::PutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/storage_api.Storage/Put");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteRequest>,
        ) -> Result<tonic::Response<super::DeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/storage_api.Storage/Delete");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for StorageClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
}
