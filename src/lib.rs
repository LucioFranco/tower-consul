//! Tower based HttpService interface to Consul

use futures::{future, Future};
use http::{Method, Request, Response, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::string::FromUtf8Error;
use tower_http::HttpService;

/// The future returned by Consul requests where `T` is the response
/// and `E` is the inner Http error.
pub type ConsulFuture<T, E> = Box<Future<Item = T, Error = Error<E>> + Send>;

/// Create new [Consul][consul] service that will talk with
/// the consul agent api. It takes some `HttpService` that takes
/// `Vec<u8>` and returns `Vec<u8>`.
///
/// Currently only the KV api is available, with more to come.
///
/// [consul]: https://www.hashicorp.com/products/consul
#[derive(Debug)]
pub struct Consul<T> {
    scheme: String,
    authority: String,
    inner: T,
}

impl<T> Consul<T>
where
    T: HttpService<Vec<u8>, ResponseBody = Vec<u8>>,
    T::Future: Send + 'static,
    T::Error: Send + 'static,
{
    /// Create a new consul client
    pub fn new(inner: T, scheme: String, authority: String) -> Self {
        Consul {
            scheme,
            authority,
            inner,
        }
    }

    /// Get a list of all Service members
    pub fn get(&mut self, key: &str) -> ConsulFuture<Vec<KVValue>, T::Error> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::GET, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Get a list of all Service members
    pub fn get_keys(&mut self, key: &str) -> ConsulFuture<Vec<String>, T::Error> {
        let url = format!("/v1/kv/{}?keys", key);
        let request = match self.build(&url, Method::GET, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Set a value of bytes into the key
    pub fn set(&mut self, key: &str, value: Vec<u8>) -> ConsulFuture<bool, T::Error> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::PUT, value) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Delete a key and its value
    pub fn delete(&mut self, key: &str) -> ConsulFuture<bool, T::Error> {
        let url = format!("/v1/kv/{}", key);
        let request = match self.build(&url, Method::DELETE, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Get a list of nodes that have registered via the provided service
    pub fn service_nodes(&mut self, service: &str) -> ConsulFuture<Vec<Service>, T::Error> {
        let url = format!("/v1/catalog/service/{}", service);
        let request = match self.build(&url, Method::GET, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Register with the current agent with the service config
    pub fn register(&mut self, service: Vec<u8>) -> ConsulFuture<(), T::Error> {
        let url = "/v1/agent/service/register";
        let request = match self.build(url, Method::PUT, service) {
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

    fn call<R>(&mut self, request: Request<Vec<u8>>) -> ConsulFuture<R, T::Error>
    where
        for<'de> R: Deserialize<'de> + Send + 'static,
    {
        let fut = self
            .inner
            .call(request)
            .map_err(|e| Error::Inner(e))
            .then(|res| match res {
                Ok(res) => Self::handle_status(res),
                Err(e) => Err(e),
            })
            .and_then(|body| serde_json::from_slice(body.as_slice()).map_err(Error::from));

        Box::new(fut)
    }

    fn build(
        &self,
        url: &str,
        method: Method,
        body: Vec<u8>,
    ) -> Result<Request<Vec<u8>>, Error<T::Error>> {
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

    fn handle_status(response: Response<T::ResponseBody>) -> Result<Vec<u8>, Error<T::Error>> {
        let status = response.status();

        if status.is_success() | status.is_redirection() | status.is_informational() {
            Ok(response.into_body())
        } else if status == StatusCode::NOT_FOUND {
            Err(Error::NotFound)
        } else if status.is_client_error() {
            let body = String::from_utf8(response.into_body())?;
            Err(Error::ConsulClient(body))
        } else if status.is_server_error() {
            let body = String::from_utf8(response.into_body())?;
            Err(Error::ConsulServer(body))
        } else {
            unreachable!("This is a bug!")
        }
    }
}

impl<T> Clone for Consul<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Consul {
            scheme: self.scheme.clone(),
            authority: self.authority.clone(),
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
/// The Error returned by the client
pub enum Error<E> {
    NotFound,
    ConsulClient(String),
    ConsulServer(String),
    Inner(E),
    Http(http::Error),
    Json(serde_json::Error),
    StringUtf8(FromUtf8Error),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
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
/// The value returned from Consul on Service requests
///
/// For more information on this go [here][value]
/// [value]: https://www.consul.io/api/agent/service.html#sample-response-1
pub struct Service {
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
