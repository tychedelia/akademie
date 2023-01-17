use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::poll_fn;
use std::marker::PhantomData;
use std::net::{SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::anyhow;
use bytes::BytesMut;
use futures::{Sink, TryStream};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, MetadataRequest, MetadataResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Decodable, DecodeError, Encodable, EncodeError, HeaderVersion, Request};
use kafka_protocol::protocol::buf::ByteBuf;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio_tower::Error;
use tokio_tower::multiplex::{
    Client, client::VecDequePendingStore, MultiplexTransport, Server, TagStore,
};
use tokio_util::codec;
use tokio_util::codec::{Framed, FramedParts, FramedWrite};
use tower::Service;


#[derive(Debug)]
pub struct KafkaClientCodec {
    correlation_id: AtomicI32,
    requests: Arc<Mutex<HashMap<i32, RequestHeader>>>,
    length_codec: codec::LengthDelimitedCodec,
}

impl KafkaClientCodec {
    #[tracing::instrument]
    pub fn new() -> Self {
        Self {
            correlation_id: Default::default(),
            requests: Default::default(),
            length_codec: codec::LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec(),
        }
    }


    #[tracing::instrument]
    fn encode_request(
        bytes: &mut BytesMut,
        header: RequestHeader,
        request: RequestKind,
        version: i16,
    ) -> Result<(), anyhow::Error> {
        match request {
            RequestKind::MetadataRequest(req) => {
                header.encode(bytes, MetadataRequest::header_version(version))?;
                req.encode(bytes, version)?;
            }
            RequestKind::ApiVersionsRequest(req) => {
                header.encode(bytes, ApiVersionsRequest::header_version(version))?;
                req.encode(bytes, version)?;
            }
            RequestKind::LeaderAndIsrRequest(req) => {
                header.encode(bytes, LeaderAndIsrRequest::header_version(version))?;
                req.encode(bytes, version)?;
            }
            RequestKind::CreateTopicsRequest(req) => {
                header.encode(bytes, CreateTopicsRequest::header_version(version))?;
                req.encode(bytes, version)?;
            }
            _ => {
                tracing::error!(?header.request_api_key, "unsupported operation!");
                Err(EncodeError)?
            },
        };

        Ok(())
    }

    #[tracing::instrument]
    fn decode_response(
        bytes: &mut BytesMut,
        api_key: ApiKey,
        version: i16,
    ) -> Result<ResponseKind, anyhow::Error> {
        match api_key {
            ApiKey::MetadataKey => {
                let res =
                    MetadataResponse::decode(bytes, version)?;
                Ok(ResponseKind::MetadataResponse(res))
            }
            ApiKey::ApiVersionsKey => {
                let res =
                    ApiVersionsResponse::decode(bytes, ApiVersionsResponse::header_version(version))?;
                Ok(ResponseKind::ApiVersionsResponse(res))
            }
            ApiKey::LeaderAndIsrKey => {
                let res =
                    LeaderAndIsrResponse::decode(bytes, LeaderAndIsrResponse::header_version(version))?;
                Ok(ResponseKind::LeaderAndIsrResponse(res))
            }
            ApiKey::CreateTopicsKey => {
                let res =
                    CreateTopicsResponse::decode(bytes, CreateTopicsResponse::header_version(version))?;
                Ok(ResponseKind::CreateTopicsResponse(res))
            }
            _ => {
                tracing::error!(?api_key, "unsupported operation!");
                Err(DecodeError)?
            },
        }
    }
}

impl codec::Encoder<(RequestHeader, RequestKind)> for KafkaClientCodec {
    type Error = anyhow::Error;

    #[tracing::instrument]
    fn encode(
        &mut self,
        item: (RequestHeader, RequestKind),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (mut header, request) = item;
        let mut bytes = BytesMut::new();
        let api_version = header.request_api_version;
        let mut requests = self.requests.lock().unwrap();
        requests.insert(header.correlation_id, header.clone());
        Self::encode_request(&mut bytes, header, request, api_version)?;
        self.length_codec
            .encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}


impl codec::Decoder for KafkaClientCodec {
    type Item = (ResponseHeader, ResponseKind);
    type Error = anyhow::Error;

    #[tracing::instrument]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src)? {
            let header = ResponseHeader::decode(&mut bytes, 1)?;
            let mut request_header = self.requests.lock().unwrap();
            let request_header = request_header
                .remove(&header.correlation_id).unwrap();
            let api_key = ApiKey::try_from(request_header.request_api_key).unwrap();
            let response =
                Self::decode_response(&mut bytes, api_key, request_header.request_api_version);
            Ok(Some((header, response?)))
        } else {
            Ok(None)
        }
    }
}

// `tokio-tower` tag store for the Kafka protocol.
#[derive(Default)]
pub struct CorrelationStore {
    correlation_ids: HashSet<i32>,
    id_gen: AtomicI32,
}

impl TagStore<(RequestHeader, RequestKind), (ResponseHeader, ResponseKind)> for CorrelationStore {
    type Tag = i32;

    fn assign_tag(mut self: Pin<&mut Self>, request: &mut (RequestHeader, RequestKind)) -> i32 {
        let tag = self.id_gen.fetch_add(1, Ordering::SeqCst);
        self.correlation_ids.insert(tag);
        request.0.correlation_id = tag;
        tag
    }

    fn finish_tag(mut self: Pin<&mut Self>, response: &(ResponseHeader, ResponseKind)) -> i32 {
        let tag = response.0.correlation_id;
        self.correlation_ids.remove(&tag);
        tag
    }
}

pub struct DisconnectedKafkaClient {
    addr: SocketAddr,
}


#[derive(Debug)]
pub struct KafkaClientError {

}

impl From<Error<MultiplexTransport<Framed<TcpStream, KafkaClientCodec>, CorrelationStore>, (RequestHeader, RequestKind)>> for KafkaClientError {
    fn from(value: Error<MultiplexTransport<Framed<TcpStream, KafkaClientCodec>, CorrelationStore>, (RequestHeader, RequestKind)>) -> Self {
        println!("{}", value);
        todo!()
    }
}


async fn ready<S: Service<Request>, Request>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}

impl DisconnectedKafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
        }
    }

    pub async fn connect(self) -> anyhow::Result<Client<MultiplexTransport<Framed<TcpStream, KafkaClientCodec>, CorrelationStore>, KafkaClientError, (RequestHeader, RequestKind)>>
    {
        let client_codec = KafkaClientCodec::new();
        let tx = TcpStream::connect(&self.addr).await.unwrap();
        let tx = Framed::new(tx, client_codec);

        let mut client = Client::builder(MultiplexTransport::new(tx, CorrelationStore::default()))
            .pending_store(VecDequePendingStore::default())
            .build();
        ready(&mut client).await.unwrap();
        Ok(client)
    }
}