//! Tower based HttpService interface to Consul

#![warn(missing_docs)]

use bytes::Bytes;
use futures::{
    future::{self, Either},
    try_ready, Async, Future, Poll,
};
use http::{Method, Request, Response, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::string::FromUtf8Error;
use tower_buffer::{Buffer, Error as BufferError, ResponseFuture, SpawnError};
use tower_http::{service::LiftService, HttpService};

/// The future returned by Consul requests where `T` is the response
/// and `E` is the inner Http error and a Box allocation is needed.
pub type BoxConsulFuture<T, E> = Box<Future<Item = T, Error = Error<E>> + Send>;

/// Create new [Consul][consul] service that will talk with
/// the consul agent api. It takes some `HttpService` that takes
/// `Bytes` and returns `Bytes`.
///
/// Currently only the KV api is available, with more to come.
///
/// [consul]: https://www.hashicorp.com/products/consul
#[derive(Clone)]
pub struct Consul<T>
where
    T: HttpService<Bytes>,
{
    scheme: String,
    authority: String,
    inner: Buffer<LiftService<T>, Request<Bytes>>,
}

/// The future that represents the eventual value
/// returned from the consul request.
pub struct ConsulFuture<T, R>
where
    for<'de> R: Deserialize<'de>,
    T: HttpService<Bytes, ResponseBody = Bytes>,
    T::Error: Sync,
{
    inner: ResponseFuture<T::Future, T::Error>,
    _pd: PhantomData<R>,
}

// == impl Consul ===

impl<T> Consul<T>
where
    T: HttpService<Bytes, ResponseBody = Bytes> + Send + 'static,
    T::Future: Send + 'static,
    T::Error: Send + Sync + 'static,
{
    /// Create a new consul client
    pub fn new(
        inner: T,
        bound: usize,
        scheme: String,
        authority: String,
    ) -> Result<Self, Error<T::Error>> {
        let inner = Buffer::new(inner.lift(), bound)?;

        Ok(Consul {
            scheme,
            authority,
            inner,
        })
    }

    /// Get a list of all Service members
    pub fn get(&mut self, key: &str) -> impl Future<Item = Vec<KVValue>, Error = Error<T::Error>> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::GET, Bytes::new()) {
            Ok(req) => req,
            Err(e) => return Either::A(future::err(e)),
        };

        Either::B(self.call(request))
    }

    /// Get a list of all Service members
    pub fn get_keys(
        &mut self,
        key: &str,
    ) -> impl Future<Item = Vec<String>, Error = Error<T::Error>> {
        let url = format!("/v1/kv/{}?keys", key);
        let request = match self.build(&url, Method::GET, Bytes::new()) {
            Ok(req) => req,
            Err(e) => return Either::A(future::err(e)),
        };

        Either::B(self.call(request))
    }

    /// Set a value of bytes into the key
    pub fn set(
        &mut self,
        key: &str,
        value: impl Into<Bytes>,
    ) -> impl Future<Item = bool, Error = Error<T::Error>> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::PUT, value.into()) {
            Ok(req) => req,
            Err(e) => return Either::A(future::err(e)),
        };

        Either::B(self.call(request))
    }

    /// Delete a key and its value
    pub fn delete(&mut self, key: &str) -> impl Future<Item = bool, Error = Error<T::Error>> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::DELETE, Bytes::new()) {
            Ok(req) => req,
            Err(e) => return Either::A(future::err(e)),
        };

        Either::B(self.call(request))
    }

    /// Get a list of nodes that have registered via the provided service
    pub fn service_nodes(
        &mut self,
        service: &str,
    ) -> impl Future<Item = Vec<ConsulService>, Error = Error<T::Error>> {
        let url = format!("/v1/catalog/service/{}", service);
        let request = match self.build(&url, Method::GET, Bytes::new()) {
            Ok(req) => req,
            Err(e) => return Either::A(future::err(e)),
        };

        Either::B(self.call(request))
    }

    /// Register with the current agent with the service config
    pub fn register(&mut self, service: impl Into<Bytes>) -> BoxConsulFuture<(), T::Error> {
        let url = "/v1/agent/service/register";
        let request = match self.build(url, Method::PUT, service.into()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        let fut = self
            .inner
            .call(request)
            .map_err(|e| Error::Inner(e))
            .then(|res| match res {
                Ok(res) => Self::handle_status(res),
                Err(e) => Err(e),
            })
            .map(|_| ());

        Box::new(fut)
    }

    fn call<R>(&mut self, request: Request<Bytes>) -> ConsulFuture<T, R>
    where
        for<'de> R: Deserialize<'de> + Send + 'static,
    {
        let fut = self.inner.call(request);

        ConsulFuture {
            inner: fut,
            _pd: PhantomData,
        }
    }

    fn build(
        &self,
        url: &str,
        method: Method,
        body: Bytes,
    ) -> Result<Request<Bytes>, Error<T::Error>> {
        let uri = Uri::builder()
            .scheme(self.scheme.as_str())
            .authority(self.authority.as_str())
            .path_and_query(url)
            .build()?;

        Request::builder()
            .uri(uri)
            .method(method)
            .body(body)
            .map_err(Error::from)
    }

    fn handle_status(response: Response<Bytes>) -> Result<Response<Bytes>, Error<T::Error>> {
        let status = response.status();

        if status.is_success() | status.is_redirection() | status.is_informational() {
            Ok(response)
        } else if status == StatusCode::NOT_FOUND {
            Err(Error::NotFound)
        } else if status.is_client_error() {
            let body = response.into_body();
            let body = String::from_utf8_lossy(&body[..]).into_owned();
            Err(Error::ConsulClient(body))
        } else if status.is_server_error() {
            let body = response.into_body();
            let body = String::from_utf8_lossy(&body[..]).into_owned();
            Err(Error::ConsulServer(body))
        } else {
            unreachable!("This is a bug!")
        }
    }
}

#[derive(Debug)]
/// The Error returned by the client
pub enum Error<E> {
    /// The requested resource does not exist
    NotFound,
    /// The consul http request returned a `4xx` response that is not
    /// a `404`
    ConsulClient(String),
    /// The consul http request returned a `5xx` response
    ConsulServer(String),
    /// The inner service returned an error
    Inner(BufferError<E>),
    /// There was an error creating and reading Response/Requests
    Http(http::Error),
    /// The error returned if the json parsing has failed
    Json(serde_json::Error),
    /// Error parsing the response string as utf8
    StringUtf8(FromUtf8Error),
    /// Error attempting to spawn the Buffer service
    SpawnError,
}

impl<E> From<serde_json::Error> for Error<E> {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl<E> From<FromUtf8Error> for Error<E> {
    fn from(e: FromUtf8Error) -> Self {
        Error::StringUtf8(e)
    }
}

impl<E> From<http::Error> for Error<E> {
    fn from(e: http::Error) -> Self {
        Error::Http(e)
    }
}

impl<E, T> From<tower_buffer::SpawnError<T>> for Error<E> {
    fn from(_: SpawnError<T>) -> Self {
        Error::SpawnError
    }
}

// == impl ConsulFuture ==

impl<T, R> Future for ConsulFuture<T, R>
where
    for<'de> R: Deserialize<'de> + Send + 'static,
    T: HttpService<Bytes, ResponseBody = Bytes>,
    T::Error: Sync,
{
    type Item = R;
    type Error = Error<T::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let response = try_ready!(self.inner.poll().map_err(|e| Error::Inner(e)));

        let status = response.status();

        let body = if status.is_success() | status.is_redirection() | status.is_informational() {
            response.into_body()
        } else if status == StatusCode::NOT_FOUND {
            return Err(Error::NotFound);
        } else if status.is_client_error() {
            let body = response.into_body();
            let body = String::from_utf8_lossy(&body[..]).into_owned();
            return Err(Error::ConsulClient(body));
        } else if status.is_server_error() {
            let body = response.into_body();
            let body = String::from_utf8_lossy(&body[..]).into_owned();
            return Err(Error::ConsulServer(body));
        } else {
            unreachable!("This is a bug!")
        };

        let body = serde_json::from_slice(&body[..])?;

        Ok(Async::Ready(body))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[allow(missing_docs)]
/// The value returned from consul
///
/// For more information on this go [here][value]
/// [value]: https://www.consul.io/api/kv.html#read-key
pub struct KVValue {
    pub create_index: i64,
    pub modify_index: i64,
    pub lock_index: i64,
    pub key: String,
    pub flags: u8,
    pub value: String,
    pub session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[allow(missing_docs)]
/// The value returned from Consul on Service requests
///
/// For more information on this go [here][value]
/// [value]: https://www.consul.io/api/agent/service.html#sample-response-1
pub struct ConsulService {
    #[serde(rename = "ServiceKind")]
    pub kind: String,
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "ServiceID")]
    pub service_id: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "ServiceTags")]
    pub tags: Vec<String>,
    #[serde(rename = "ServiceMeta")]
    pub meta: HashMap<String, String>,
    pub node: String,
    pub address: String,
    pub datacenter: String,
}
