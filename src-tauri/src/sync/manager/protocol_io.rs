use super::*;

pub(super) fn authenticate_local(
    manager: &SyncManager,
    group_id: &str,
    access_token: &[u8],
) -> Result<GroupSecrets> {
    let group = manager
        .identity
        .snapshot()
        .group
        .ok_or_else(|| RemoteSyncError {
            code: ErrorCode::Unauthorized,
            message: "device is not paired".to_owned(),
        })?;
    if group.group_id != group_id || group.access_token_bytes()? != access_token {
        return Err(RemoteSyncError {
            code: ErrorCode::Unauthorized,
            message: "unauthorized".to_owned(),
        }
        .into());
    }
    Ok(group)
}

pub(super) async fn call(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await.context("read sync response")
}

/// Bounds LAN control-response latency without constraining request or blob transfer duration.
pub(super) async fn call_lan(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) =
        tokio::time::timeout(LAN_CONTROL_REQUEST_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow::Error::new(LanAttemptTimeout).context("打开局域网同步流超时"))??;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    let length = match tokio::time::timeout(LAN_CONTROL_REQUEST_TIMEOUT, recv.read_u32()).await {
        Ok(length) => length.context("读取局域网同步响应长度")? as usize,
        Err(_) => {
            return Err(anyhow::Error::new(LanAttemptTimeout).context("等待局域网同步响应超时"));
        }
    };
    if length > MAX_FRAME_BYTES {
        bail!("LAN response frame is too large: {length} bytes");
    }
    let mut encoded = vec![0; length];
    match tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read_exact(&mut encoded)).await {
        Ok(result) => result.context("读取局域网同步响应")?,
        Err(_) => {
            return Err(anyhow::Error::new(LanAttemptTimeout).context("读取局域网同步响应超时"));
        }
    }
    minicbor::decode(&encoded).context("decode LAN sync response")
}

pub(super) fn cloud_attempt_timeout(phase: CloudAttemptPhase, language: Language) -> anyhow::Error {
    anyhow::Error::new(CloudAttemptTimeout { phase, language })
}

/// Writes every byte while resetting the timeout whenever the transport makes progress.
pub(super) async fn write_all_with_cloud_idle_timeout<W>(
    writer: &mut W,
    mut bytes: &[u8],
    idle_timeout: Duration,
    language: Language,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while !bytes.is_empty() {
        let written = match tokio::time::timeout(idle_timeout, writer.write(bytes)).await {
            Ok(result) => result.context("write cloud sync request")?,
            Err(_) => {
                return Err(cloud_attempt_timeout(
                    CloudAttemptPhase::WriteRequest,
                    language,
                ));
            }
        };
        if written == 0 {
            return Err(anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "cloud sync request writer made no progress",
            )));
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

/// Encodes one cloud control request and bounds only periods without write progress.
pub(super) async fn write_cloud_request<W>(
    writer: &mut W,
    request: &Request,
    idle_timeout: Duration,
    language: Language,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = minicbor::to_vec(request).context("encode cloud sync request")?;
    if encoded.len() > MAX_FRAME_BYTES {
        bail!("cloud request frame is too large: {} bytes", encoded.len());
    }
    let length = u32::try_from(encoded.len())?.to_be_bytes();
    write_all_with_cloud_idle_timeout(writer, &length, idle_timeout, language).await?;
    write_all_with_cloud_idle_timeout(writer, &encoded, idle_timeout, language).await?;
    match tokio::time::timeout(idle_timeout, writer.flush()).await {
        Ok(result) => result.context("flush cloud sync request")?,
        Err(_) => {
            return Err(cloud_attempt_timeout(
                CloudAttemptPhase::WriteRequest,
                language,
            ));
        }
    }
    Ok(())
}

/// Reads every byte while resetting the timeout whenever the transport makes progress.
pub(super) async fn read_exact_with_cloud_idle_timeout<R>(
    reader: &mut R,
    buffer: &mut [u8],
    idle_timeout: Duration,
    language: Language,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut offset = 0;
    while offset < buffer.len() {
        let read =
            match tokio::time::timeout(idle_timeout, reader.read(&mut buffer[offset..])).await {
                Ok(result) => result.context("read cloud sync response")?,
                Err(_) => {
                    return Err(cloud_attempt_timeout(
                        CloudAttemptPhase::ReadResponseBody,
                        language,
                    ));
                }
            };
        if read == 0 {
            return Err(anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "cloud sync response ended early",
            )));
        }
        offset += read;
    }
    Ok(())
}

/// Reads one cloud response with separate header and body-progress deadlines.
pub(super) async fn read_cloud_response<R>(
    reader: &mut R,
    header_timeout: Duration,
    body_idle_timeout: Duration,
    language: Language,
) -> Result<Response>
where
    R: AsyncRead + Unpin,
{
    let length = match tokio::time::timeout(header_timeout, reader.read_u32()).await {
        Ok(result) => result.context("read cloud response frame length")? as usize,
        Err(_) => {
            return Err(cloud_attempt_timeout(
                CloudAttemptPhase::ReadResponseHeader,
                language,
            ));
        }
    };
    if length > MAX_FRAME_BYTES {
        bail!("cloud response frame is too large: {length} bytes");
    }
    let mut encoded = vec![0; length];
    read_exact_with_cloud_idle_timeout(reader, &mut encoded, body_idle_timeout, language).await?;
    minicbor::decode(&encoded).context("decode cloud sync response")
}

/// Bounds every cloud control phase without limiting a response that keeps making progress.
pub(super) async fn call_cloud(
    connection: &Connection,
    request: Request,
    language: Language,
) -> Result<Response> {
    let (mut send, mut recv) =
        match tokio::time::timeout(CLOUD_CONTROL_REQUEST_TIMEOUT, connection.open_bi()).await {
            Ok(result) => result.context("open cloud sync stream")?,
            Err(_) => {
                return Err(cloud_attempt_timeout(
                    CloudAttemptPhase::OpenStream,
                    language,
                ));
            }
        };
    write_cloud_request(
        &mut send,
        &request,
        CLOUD_CONTROL_REQUEST_IDLE_TIMEOUT,
        language,
    )
    .await?;
    send.finish()?;
    read_cloud_response(
        &mut recv,
        CLOUD_CONTROL_REQUEST_TIMEOUT,
        BLOB_IDLE_TIMEOUT,
        language,
    )
    .await
}

pub(super) fn is_cloud_attempt_timeout(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CloudAttemptTimeout>().is_some()
}

pub(super) fn should_invalidate_cloud_connection(
    connection: &Connection,
    error: &anyhow::Error,
) -> bool {
    connection.close_reason().is_some() || is_cloud_attempt_timeout(error)
}

pub(super) fn is_lan_attempt_timeout(error: &anyhow::Error) -> bool {
    error.downcast_ref::<LanAttemptTimeout>().is_some()
}

/// A failed card stream does not invalidate the shared connection while it remains authenticated.
pub(super) fn is_shared_sync_failure(connection: &Connection, error: &anyhow::Error) -> bool {
    connection.close_reason().is_some()
        || error
            .downcast_ref::<RemoteSyncError>()
            .is_some_and(|failure| failure.code == ErrorCode::Unauthorized)
}

/// Validates an approval, waits for the approver's durable commit, then changes the local space.
pub(super) async fn call_join(
    connection: &Connection,
    request: JoinRequest,
    apply: Option<(&SyncManager, bool, bool)>,
) -> Result<JoinResponse> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let request_id = request.request_id.clone();
    write_frame(&mut send, &request).await?;
    let response: JoinResponse = read_frame(&mut recv).await.context("读取加入申请结果")?;
    let validation_error = match (&response, apply) {
        (JoinResponse::Approved { bootstrap }, Some((manager, replace_existing, _))) => {
            let result = if bootstrap.inviter.endpoint_id != connection.remote_id().to_string() {
                Err(anyhow::anyhow!("批准设备身份与加密连接不一致"))
            } else {
                manager.validate_pairing_bootstrap(bootstrap, replace_existing)
            };
            result.err().map(|error| error.to_string())
        }
        _ => None,
    };
    write_frame(
        &mut send,
        &JoinAcknowledgement {
            version: JOIN_PROTOCOL_VERSION,
            request_id,
            accepted: validation_error.is_none(),
            error: validation_error.clone(),
        },
    )
    .await?;
    let completion: JoinCompletion =
        tokio::time::timeout(JOIN_COMMIT_TIMEOUT, read_frame(&mut recv))
            .await
            .context("确认加入申请结果超时")?
            .context("确认加入申请结果")?;
    let outcome = if let Some(message) = validation_error {
        Err(anyhow::anyhow!(message))
    } else {
        match completion {
            JoinCompletion::Committed => {
                if let (
                    JoinResponse::Approved { bootstrap },
                    Some((manager, replace_existing, preserve_runtime)),
                ) = (&response, apply)
                {
                    manager
                        .apply_pairing_bootstrap(bootstrap, replace_existing, preserve_runtime)
                        .await?;
                }
                Ok(response)
            }
            JoinCompletion::Error { message } => Ok(JoinResponse::Error { message }),
        }
    };
    send.finish()?;
    let stopped = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, send.stopped())
        .await
        .context("等待批准设备接收确认超时")??;
    if stopped.is_some() {
        bail!("批准设备中止了加入确认");
    }
    outcome
}
