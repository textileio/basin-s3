// Copyright 2022-2024 Protocol Labs
// SPDX-License-Identifier: Apache-2.0, MIT

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use base64::{alphabet, Engine};
use base64::engine::{DecodePaddingMode, GeneralPurposeConfig};
use base64::engine::GeneralPurpose;
use cid::Cid;
use fendermint_actor_objectstore::Object;
use fendermint_crypto::SecretKey;
use fendermint_rpc::client::BoundFendermintClient;
use fendermint_rpc::client::FendermintClient;
use fendermint_rpc::message::{GasParams, MessageFactory};
use fendermint_rpc::tx::{
    AsyncResponse, BoundClient, CallClient, CommitResponse, SyncResponse, TxAsync, TxClient,
    TxCommit, TxSync,
};
use fendermint_vm_actor_interface::eam::EthAddress;
use fendermint_vm_core::chainid;
use fendermint_vm_message::chain::ChainMessage;
use fendermint_vm_message::query::FvmQueryHeight;
use fvm_shared::address::Address;
use fvm_shared::econ::TokenAmount;
use num_traits::Zero;
use serde::Serialize;
use tendermint::abci::response::DeliverTx;
use tendermint::block::Height;
use tendermint::Hash;
use tendermint_rpc::HttpClient;

/// A [`GeneralPurpose`] engine using the [`alphabet::STANDARD`] base64 alphabet
/// padding bytes when writing but requireing no padding when reading.
const B64_ENGINE: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Encode bytes in a format that the Genesis deserializer can handle.
pub fn to_b64(bz: &[u8]) -> String {
    B64_ENGINE.encode(bz)
}

pub fn from_b64(b64: &str) -> anyhow::Result<Vec<u8>> {
    Ok(B64_ENGINE.decode(b64)?)
}

fn b64_to_secret(b64: &str) -> anyhow::Result<SecretKey> {
    let bz = from_b64(b64)?;
    let sk = SecretKey::try_from(bz)?;
    Ok(sk)
}

pub fn read_secret_key(secret_key: &PathBuf) -> anyhow::Result<SecretKey> {
    let b64 = std::fs::read_to_string(secret_key).context("failed to read secret key")?;
    let sk = b64_to_secret(&b64).context("failed to parse secret key")?;
    Ok(sk)
}

#[derive(Debug, Clone)]
pub enum AccountKind {
    Regular,
    Ethereum,
}

/// Arguments common to transactions and transfers.
#[derive(Debug, Clone)]
pub struct TransArgs {
    /// Name of chain the for which the message will be signed.
    pub chain_name: String,
    /// Amount of tokens to send, in full FIL, not atto.
    pub value: TokenAmount,
    /// Path to the secret key of the sender to sign the transaction.
    pub secret_key: PathBuf,
    /// Indicate whether its a regular or ethereum account.
    pub account_kind: AccountKind,
    /// Sender account nonce.
    pub sequence: u64,
    /// Maximum amount of gas that can be charged.
    pub gas_limit: u64,
    /// Price of gas.
    ///
    /// Any discrepancy between this and the base fee is paid for
    /// by the validator who puts the transaction into the block.
    pub gas_fee_cap: TokenAmount,
    /// Gas premium.
    pub gas_premium: TokenAmount,
    /// Whether to wait for the results from Tendermint or not.
    pub broadcast_mode: BroadcastMode,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BroadcastMode {
    /// Do not wait for the results.
    Async,
    /// Wait for the result of `check_tx`.
    Sync,
    /// Wait for the result of `deliver_tx`.
    Commit,
}

pub enum BroadcastResponse<T> {
    Async(AsyncResponse<T>),
    Sync(SyncResponse<T>),
    Commit(CommitResponse<T>),
}

pub struct BroadcastModeWrapper(BroadcastMode);

impl fendermint_rpc::tx::BroadcastMode for BroadcastModeWrapper {
    type Response<T> = BroadcastResponse<T>;
}

pub struct TransClient {
    pub(crate) inner: BoundFendermintClient<HttpClient>,
    broadcast_mode: BroadcastModeWrapper,
}

impl TransClient {
    pub fn new(client: FendermintClient, args: &TransArgs) -> anyhow::Result<Self> {
        let sk = read_secret_key(&args.secret_key)?;
        let addr = to_address(&sk, &args.account_kind)?;
        let chain_id = chainid::from_str_hashed(&args.chain_name)?;
        let mf = MessageFactory::new(sk, addr, args.sequence, chain_id);
        let client = client.bind(mf);
        let client = Self {
            inner: client,
            broadcast_mode: BroadcastModeWrapper(args.broadcast_mode),
        };
        Ok(client)
    }
}

impl BoundClient for TransClient {
    fn message_factory_mut(&mut self) -> &mut MessageFactory {
        self.inner.message_factory_mut()
    }
}

#[async_trait]
impl TxClient<BroadcastModeWrapper> for TransClient {
    async fn perform<F, T>(&self, msg: ChainMessage, f: F) -> anyhow::Result<BroadcastResponse<T>>
    where
        F: FnOnce(&DeliverTx) -> anyhow::Result<T> + Sync + Send,
        T: Sync + Send,
    {
        match self.broadcast_mode.0 {
            BroadcastMode::Async => {
                let res = TxClient::<TxAsync>::perform(&self.inner, msg, f).await?;
                Ok(BroadcastResponse::Async(res))
            }
            BroadcastMode::Sync => {
                let res = TxClient::<TxSync>::perform(&self.inner, msg, f).await?;
                Ok(BroadcastResponse::Sync(res))
            }
            BroadcastMode::Commit => {
                let res = TxClient::<TxCommit>::perform(&self.inner, msg, f).await?;
                Ok(BroadcastResponse::Commit(res))
            }
        }
    }
}

pub fn gas_params(args: &TransArgs) -> GasParams {
    GasParams {
        gas_limit: args.gas_limit,
        gas_fee_cap: args.gas_fee_cap.clone(),
        gas_premium: args.gas_premium.clone(),
    }
}

fn to_address(sk: &SecretKey, kind: &AccountKind) -> anyhow::Result<Address> {
    let pk = sk.public_key().serialize();
    match kind {
        AccountKind::Regular => Ok(Address::new_secp256k1(&pk)?),
        AccountKind::Ethereum => Ok(Address::from(EthAddress::new_secp256k1(&pk)?)),
    }
}

#[derive(Clone, Debug, Serialize)]
enum TxnStatus {
    Pending,
    Committed,
}

#[derive(Clone, Debug, Serialize)]
struct Txn {
    pub status: TxnStatus,
    pub hash: Hash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<Height>,
    #[serde(skip_serializing_if = "i64::is_zero")]
    pub gas_used: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

impl Txn {
    fn pending(hash: Hash) -> Self {
        Txn {
            status: TxnStatus::Pending,
            hash,
            height: None,
            gas_used: 0,
            state: None,
        }
    }

    fn committed(hash: Hash, height: Height, gas_used: i64, state: Cid) -> Self {
        Txn {
            status: TxnStatus::Committed,
            hash,
            height: Some(height),
            gas_used,
            state: Some(state.to_string()),
        }
    }
}

/// Create a client, make a call to Tendermint with a closure, then maybe extract some JSON
/// depending on the return value, finally return the result in JSON.
async fn broadcast<F>(client: FendermintClient, args: TransArgs, f: F) -> anyhow::Result<Txn>
where
    F: FnOnce(
        TransClient,
        TokenAmount,
        GasParams,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<BroadcastResponse<Cid>>> + Send>>,
{
    let client = TransClient::new(client, &args)?;
    let gas_params = gas_params(&args);
    let res = f(client, TokenAmount::default(), gas_params).await?;
    Ok(match res {
        BroadcastResponse::Async(res) => Txn::pending(res.response.hash),
        BroadcastResponse::Sync(res) => {
            if res.response.code.is_err() {
                return Err(anyhow!(res.response.log));
            }
            Txn::pending(res.response.hash)
        }
        BroadcastResponse::Commit(res) => {
            if res.response.check_tx.code.is_err() {
                return Err(anyhow!(res.response.check_tx.log));
            } else if res.response.deliver_tx.code.is_err() {
                return Err(anyhow!(res.response.deliver_tx.log));
            }
            Txn::committed(
                res.response.hash,
                res.response.height,
                res.response.deliver_tx.gas_used,
                res.return_data.unwrap_or_default(),
            )
        }
    })
}

async fn os_put(
    client: FendermintClient,
    args: TransArgs,
    key: String,
    content: Cid,
) -> anyhow::Result<Txn> {
    broadcast(client, args, |mut client, value, gas_params| {
        Box::pin(async move { client.os_put(key, content, value, gas_params).await })
    })
    .await
}

async fn os_delete(client: FendermintClient, args: TransArgs, key: String) -> anyhow::Result<Txn> {
    broadcast(client, args, |mut client, value, gas_params| {
        Box::pin(async move { client.os_delete(key, value, gas_params).await })
    })
    .await
}

async fn os_get(
    client: FendermintClient,
    args: TransArgs,
    key: String,
    height: u64,
) -> anyhow::Result<Option<Object>> {
    let mut client = TransClient::new(client, &args)?;
    let gas_params = gas_params(&args);
    let h = FvmQueryHeight::from(height);

    let res = client
        .inner
        .os_get_call(key, TokenAmount::default(), gas_params, h)
        .await?;

    Ok(res.return_data)
}

async fn os_list(
    client: FendermintClient,
    args: TransArgs,
    height: u64,
) -> anyhow::Result<Option<Vec<(Vec<u8>, Object)>>> {
    let mut client = TransClient::new(client, &args)?;
    let gas_params = gas_params(&args);
    let h = FvmQueryHeight::from(height);

    let res = client
        .inner
        .os_list_call(TokenAmount::default(), gas_params, h)
        .await?;

    Ok(res.return_data)
}
