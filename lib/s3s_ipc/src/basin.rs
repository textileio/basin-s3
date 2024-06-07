use adm_provider::{json_rpc::JsonRpcProvider, message::GasParams};
use adm_signer::{Signer, SubnetID, Wallet};
use async_trait::async_trait;
use fendermint_crypto::SecretKey;
use fendermint_vm_message::{chain::ChainMessage, signed::SignedMessage};
use fvm_ipld_encoding::RawBytes;
use fvm_shared::{address::Address, MethodNum};
use s3s::dto::PartNumber;
use std::{path::PathBuf, sync::Arc};
use tendermint_rpc::Client;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct Basin<C: Client + Send + Sync> {
    pub root: PathBuf,
    pub provider: JsonRpcProvider<C>,
    pub wallet: Option<BasinWallet>,
    pub is_read_only: bool,
}

impl<C> Basin<C>
where
    C: Client + Send + Sync,
{
    pub fn new(
        root: PathBuf,
        provider: JsonRpcProvider<C>,
        wallet: Option<BasinWallet>,
    ) -> anyhow::Result<Self> {
        let is_read_only = wallet.is_none();
        Ok(Self {
            root,
            provider,
            wallet,
            is_read_only,
        })
    }

    pub fn get_upload_path(&self, upload_id: &Uuid) -> PathBuf {
        self.root.join(format!("upload-{upload_id}.json"))
    }

    pub fn get_upload_part_path(&self, upload_id: &Uuid, part_number: PartNumber) -> PathBuf {
        self.root
            .join(format!(".upload-{upload_id}.part-{part_number}.json"))
    }
}

#[derive(Clone)]
pub struct BasinWallet {
    address: Address,
    subnet_id: Option<SubnetID>,
    secret_key: Option<SecretKey>,
    wallet: Arc<RwLock<Wallet>>,
}

impl BasinWallet {
    #[must_use]
    pub fn new(wallet: Wallet) -> Self {
        BasinWallet {
            address: wallet.address(),
            subnet_id: wallet.subnet_id(),
            secret_key: wallet.secret_key(),
            wallet: Arc::new(RwLock::new(wallet)),
        }
    }
}

#[async_trait]
impl Signer for BasinWallet {
    fn address(&self) -> Address {
        self.address
    }

    fn secret_key(&self) -> Option<SecretKey> {
        self.clone().secret_key
    }

    fn subnet_id(&self) -> Option<SubnetID> {
        self.clone().subnet_id
    }

    async fn transaction(
        &mut self,
        to: Address,
        value: fvm_shared::econ::TokenAmount,
        method_num: MethodNum,
        params: RawBytes,
        object: Option<fendermint_vm_message::signed::Object>,
        gas_params: GasParams,
    ) -> anyhow::Result<ChainMessage> {
        self.wallet
            .write()
            .await
            .transaction(to, value, method_num, params, object, gas_params)
            .await
    }

    fn sign_message(
        &self,
        message: fvm_shared::message::Message,
        object: Option<fendermint_vm_message::signed::Object>,
    ) -> anyhow::Result<SignedMessage> {
        let signed = SignedMessage::new_secp256k1(
            message,
            object,
            self.secret_key.as_ref().unwrap(),
            &self.subnet_id.as_ref().unwrap().chain_id(),
        )?;
        Ok(signed)
    }

    fn verify_message(
        &self,
        message: &fvm_shared::message::Message,
        object: &Option<fendermint_vm_message::signed::Object>,
        signature: &fvm_shared::crypto::signature::Signature,
    ) -> anyhow::Result<()> {
        SignedMessage::verify_signature(
            message,
            object,
            signature,
            &self.subnet_id.as_ref().unwrap().chain_id(),
        )?;
        Ok(())
    }
}
