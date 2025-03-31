use crate::{Connection, Frame};

use tracing::{debug, instrument};

/// 代表一个未知命令，这个不是一个真正的Redis命令
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.get_name()));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
