use futures::{Async, Future, Poll, Stream};
use hyper::{client::HttpConnector, Body, Client, Request, Response};
use std::panic;
use std::process::{Command, Stdio};
use tokio::runtime::Runtime;
use tower_consul::Consul;
use tower_service::Service;

static CONSUL_ADDRESS: &'static str = "http://127.0.0.1:8500";

#[test]
fn _check_consul() {
    Command::new("consul")
        .arg("--version")
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
        .wait()
        .expect("Unable to find consul. Consul needs to be available in the path");
}

#[test]
fn get_empty() {
    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.get("tower-consul/test-key"));

    assert!(response.is_err());
}

#[test]
fn get_one() {
    consul_put("tower-consul/test-key", "test-value");

    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.get("tower-consul/test-key"));

    let mut values = response.unwrap();

    let value = values.pop().unwrap();

    assert_eq!(value.key, "tower-consul/test-key");

    consul_del("tower-consul/test-key");
}

#[test]
fn get_keys_empty() {
    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.get_keys("tower-consul/test-key-not-found"));

    assert!(response.is_err());
}

#[test]
fn get_keys_success() {
    consul_put("tower-consul/test-keys/some-key-1", "value-1");
    consul_put("tower-consul/test-keys/some-key-2", "value-2");

    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.get_keys("tower-consul/test-keys"));

    response.unwrap();

    consul_del("tower-consul/test-keys/some-key-1");
    consul_del("tower-consul/test-keys/some-key-2");
}

#[test]
fn set_key() {
    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.set(
        "tower-consul/test-set",
        Vec::from("hello, world".as_bytes()),
    ));

    assert!(response.unwrap());

    consul_del("tower-consul/test-set");
}

#[test]
fn delete_key() {
    consul_put("tower-consul/test-set", "some-value-to-be-deleted");

    let mut client = client();

    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(client.delete("tower-consul/test-delete"));

    assert!(response.unwrap());

    let response = rt.block_on(client.get("tower-consul/test-delete"));

    assert!(response.is_err());
}

fn client() -> Consul<Hyper> {
    Consul::new(Hyper(Client::new()), CONSUL_ADDRESS)
}

struct Hyper(Client<HttpConnector, Body>);

impl Service<Request<Vec<u8>>> for Hyper {
    type Response = Response<Vec<u8>>;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Request<Vec<u8>>) -> Self::Future {
        let f = self
            .0
            .request(req.map(Body::from))
            .and_then(|res| {
                let status = res.status().clone();
                let headers = res.headers().clone();

                res.into_body().concat2().join(Ok((status, headers)))
            })
            .and_then(|(body, (status, _headers))| {
                Ok(Response::builder()
                    .status(status)
                    // .headers(headers)
                    .body(body.to_vec())
                    .unwrap())
            });

        Box::new(f)
    }
}

fn consul_put(key: &str, value: &str) {
    Command::new("consul")
        .arg("kv")
        .arg("put")
        .arg(key)
        .arg(value)
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}

fn consul_del(key: &str) {
    Command::new("consul")
        .arg("kv")
        .arg("delete")
        .arg(key)
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}
