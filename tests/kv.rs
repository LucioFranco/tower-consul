use bytes::Bytes;
use futures::{future, Future, Stream};
use hyper::{Body, Client, Request, Response};
use serde::Serialize;
use std::panic;
use std::process::{Command, Stdio};
use tokio::runtime::Runtime;
use tower_consul::Consul;
use tower_util::{service_fn, ServiceFn};

static CONSUL_ADDRESS: &'static str = "127.0.0.1:8500";

#[test]
fn check_consul() {
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
    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client.get("tower-consul/test-key")
    }));

    assert!(response.is_err());
}

#[test]
fn get_one() {
    consul_put("tower-consul/test-key", "test-value");

    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);
        client.get("tower-consul/test-key")
    }));

    let mut values = response.unwrap();
    let value = values.pop().unwrap();
    assert_eq!(value.key, "tower-consul/test-key");

    consul_del("tower-consul/test-key");
}

#[test]
fn get_keys_empty() {
    let mut rt = Runtime::new().unwrap();
    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client.get_keys("tower-consul/test-key-not-found")
    }));

    assert!(response.is_err());
}

#[test]
fn get_keys_success() {
    consul_put("tower-consul/test-keys/some-key-1", "value-1");
    consul_put("tower-consul/test-keys/some-key-2", "value-2");

    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);
        client.get_keys("tower-consul/test-keys")
    }));

    response.unwrap();

    consul_del("tower-consul/test-keys/some-key-1");
    consul_del("tower-consul/test-keys/some-key-2");
}

#[test]
fn set_key() {
    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client.set(
            "tower-consul/test-set",
            Vec::from("hello, world".as_bytes()),
        )
    }));

    assert!(response.unwrap());

    consul_del("tower-consul/test-set");
}

#[test]
fn delete_key() {
    consul_put("tower-consul/test-set", "some-value-to-be-deleted");

    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client
            .delete("tower-consul/test-delete")
            .and_then(move |_| client.get("tower-consul/test-delete"))
    }));

    assert!(response.is_err());
}

#[test]
fn service_nodes() {
    consul_register();

    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client.service_nodes("tower-consul")
    }));

    let services = response.unwrap();

    assert_eq!(services.len(), 1);

    consul_deregister();
}

#[test]
fn register_service() {
    #[derive(Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct MockService {
        name: String,
        tags: Vec<String>,
        port: u16,
    }

    let mock = MockService {
        name: "tower-consul-mock-service".into(),
        tags: vec!["mock".into(), "test".into()],
        port: 12345,
    };

    let buf = serde_json::to_vec(&mock).unwrap();

    let mut rt = Runtime::new().unwrap();

    let response = rt.block_on(future::lazy(|| {
        let mut client = client(hyper);

        client.register(buf)
    }));

    assert!(response.is_ok());
}

type ResponseFuture = Box<Future<Item = Response<Bytes>, Error = hyper::Error> + Send + 'static>;

fn client<F>(f: F) -> Consul<ServiceFn<F>>
where
    F: Fn(Request<Bytes>) -> ResponseFuture + Send + 'static,
{
    let hyper = service_fn(f);

    match Consul::new(hyper, 100, "http".into(), CONSUL_ADDRESS.into()) {
        Ok(c) => c,
        Err(_) => panic!("Unable to spawn!"),
    }
}

fn hyper(req: Request<Bytes>) -> ResponseFuture {
    let client = Client::new();

    let fut = client
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
                .body(Bytes::from(body))
                .unwrap())
        });

    Box::new(fut)
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

fn consul_register() {
    Command::new("consul")
        .arg("services")
        .arg("register")
        .arg("tests/mock-service.json")
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}

fn consul_deregister() {
    Command::new("consul")
        .arg("services")
        .arg("deregister")
        .arg("tests/mock-service.json")
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}
