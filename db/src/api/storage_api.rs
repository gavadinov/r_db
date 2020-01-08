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
    #[prost(int64, tag = "2")]
    pub key: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(int64, tag = "1")]
    pub shard_id: i64,
    #[prost(int64, tag = "2")]
    pub key: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(string, tag = "1")]
    pub key: std::string::String,
    #[prost(string, tag = "2")]
    pub val: std::string::String,
}
#[doc = r" Generated server implementations."]
pub mod storage_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with StorageServer."]
    #[async_trait]
    pub trait Storage: Send + Sync + 'static {
        async fn get(
            &self,
            request: tonic::Request<super::GetRequest>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
        async fn put(
            &self,
            request: tonic::Request<super::PutRequest>,
        ) -> Result<tonic::Response<super::PutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteRequest>,
        ) -> Result<tonic::Response<super::DeleteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
    }
    #[derive(Debug)]
    #[doc(hidden)]
    pub struct StorageServer<T: Storage> {
        inner: Arc<T>,
    }
    impl<T: Storage> StorageServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            Self { inner }
        }
    }
    impl<T: Storage> Service<http::Request<HyperBody>> for StorageServer<T> {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<HyperBody>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/storage_api.Storage/Get" => {
                    struct GetSvc<T: Storage>(pub Arc<T>);
                    impl<T: Storage> tonic::server::UnaryService<super::GetRequest> for GetSvc<T> {
                        type Response = super::GetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/storage_api.Storage/Put" => {
                    struct PutSvc<T: Storage>(pub Arc<T>);
                    impl<T: Storage> tonic::server::UnaryService<super::PutRequest> for PutSvc<T> {
                        type Response = super::PutResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PutRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.put(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = PutSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/storage_api.Storage/Delete" => {
                    struct DeleteSvc<T: Storage>(pub Arc<T>);
                    impl<T: Storage> tonic::server::UnaryService<super::DeleteRequest> for DeleteSvc<T> {
                        type Response = super::DeleteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Storage> Clone for StorageServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Storage> tonic::transport::ServiceName for StorageServer<T> {
        const NAME: &'static str = "storage_api.Storage";
    }
}
