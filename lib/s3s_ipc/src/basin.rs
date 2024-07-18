use adm_provider::json_rpc::JsonRpcProvider;
use adm_signer::Signer;
use s3s::dto::PartNumber;
use std::path::PathBuf;
use tendermint_rpc::Client;
use uuid::Uuid;

pub struct Basin<C: Client + Send + Sync, S: Signer> {
    pub root: PathBuf,
    pub provider: JsonRpcProvider<C>,
    pub wallet: Option<S>,
    pub is_read_only: bool,
}

impl<C, S> Basin<C, S>
where
    C: Client + Send + Sync,
    S: Signer,
{
    pub fn new(
        root: PathBuf,
        provider: JsonRpcProvider<C>,
        wallet: Option<S>,
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
