use futures::{future, Async, Future, Poll, Stream};
use hyper::{client::HttpConnector, Body, Client, Request, Response};
use tower_buffer::Buffer;
use tower_consul::Consul;
use tower_service::Service;

fn main() {
    tokio::run(future::lazy(|| get_services()))
}

fn get_services() -> impl Future<Item = (), Error = ()> {
    let client = match Buffer::new(Hyper(Client::new()), 100) {
        Ok(c) => c,
        Err(_) => panic!("Unable to spawn"),
    };

    let mut consul = Consul::new(client, "http".into(), "localhost:8500".into());

    consul
        .get("my-key")
        .and_then(|value| {
            println!("value: {:?}", value);
            Ok(())
        })
        .map_err(|e| panic!("{:?}", e))
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
