//! Tower based HttpService interface to Consul

use futures::{future, Future};
use http::{Method, Request, Response, StatusCode};
use serde::Deserialize;
use serde_derive::{Deserialize, Serialize};
use std::string::FromUtf8Error;
use tower_http::HttpService;
use url::Url;

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
pub struct Consul<T> {
    base: Url,
    inner: T,
}

impl<T> Consul<T>
where
    T: HttpService<Vec<u8>, ResponseBody = Vec<u8>>,
    T::Future: Send + 'static,
    T::Error: Send + 'static,
{
    /// Create a new consul client
    pub fn new(inner: T, base_url: &str) -> Self {
        let base = Url::parse(base_url).unwrap();
        Consul { inner, base }
    }

    /// Get a list of all Service members
    pub fn get(&mut self, key: &str) -> ConsulFuture<Vec<KVGet>, T::Error> {
        let url = "/v1/kv/".to_owned() + key;
        let request = match self.build(url, Method::GET, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Get a list of all Service members
    pub fn get_keys(&mut self, key: &str) -> ConsulFuture<Vec<String>, T::Error> {
        let url = "/v1/kv/".to_owned() + key + "?keys";
        let request = match self.build(url, Method::GET, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Set a value of bytes into the key
    pub fn set(&mut self, key: &str, value: Vec<u8>) -> ConsulFuture<bool, T::Error> {
        let url = "/v1/kv/".to_owned() + key;

        let request = match self.build(url, Method::PUT, value) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
    }

    /// Delete a key and its value
    pub fn delete(&mut self, key: &str) -> ConsulFuture<bool, T::Error> {
        let url = "/v1/kv/".to_owned() + key;

        let request = match self.build(url, Method::DELETE, Vec::new()) {
            Ok(req) => req,
            Err(e) => return Box::new(future::lazy(move || Box::new(future::err(e)))),
        };

        self.call(request)
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
        url: String,
        method: Method,
        body: Vec<u8>,
    ) -> Result<Request<Vec<u8>>, Error<T::Error>> {
        let url = self.base.join(url.as_str())?;

        Request::builder()
            .uri(url.as_str())
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

#[derive(Debug)]
pub enum Error<E> {
    NotFound,
    ConsulClient(String),
    ConsulServer(String),
    Inner(E),
    Http(http::Error),
    Url(url::ParseError),
    Json(serde_json::Error),
    StringUtf8(FromUtf8Error),
}

impl<E> From<serde_json::Error> for Error<E> {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl<E> From<url::ParseError> for Error<E> {
    fn from(e: url::ParseError) -> Self {
        Error::Url(e)
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
#[allow(non_snake_case)]
pub struct KVGet {
    CreateIndex: i64,
    ModifyIndex: i64,
    LockIndex: i64,
    Key: String,
    Flags: u8,
    Value: String,
    Session: Option<String>,
}
