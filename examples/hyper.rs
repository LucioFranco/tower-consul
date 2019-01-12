use bytes::Bytes;
use futures::{future, Future, Stream};
use hyper::{Body, Client, Request, Response};
use tower_consul::Consul;
use tower_util::ServiceFn;

static CONSUL_ADDRESS: &'static str = "127.0.0.1:8500";

fn main() {
    hyper::rt::run(future::lazy(|| get_services()))
}

fn get_services() -> impl Future<Item = (), Error = ()> {
    let hyper = ServiceFn::new(hyper);

    let mut consul = match Consul::new(hyper, 100, "http".into(), CONSUL_ADDRESS.into()) {
        Ok(c) => c,
        Err(_) => panic!("Unable to spawn!"),
    };

    consul
        .get("my-key")
        .and_then(|value| {
            println!("value: {:?}", value);
            Ok(())
        })
        .map_err(|e| panic!("{:?}", e))
}

fn hyper(req: Request<Bytes>) -> impl Future<Item = Response<Bytes>, Error = hyper::Error> {
    let client = Client::new();

    client
        .request(req.map(Body::from))
        .and_then(|res| {
            let status = res.status().clone();
            res.into_body().concat2().join(Ok(status))
        })
        .and_then(|(body, status)| {
            Ok(Response::builder()
                .status(status)
                .body(body.into())
                .unwrap())
        })
}
