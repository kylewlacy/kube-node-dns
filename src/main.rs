use std::collections::HashSet;

use clap::Parser;
use joinery::Joinable as _;
use miette::{Context as _, IntoDiagnostic as _};
use network_interface::NetworkInterfaceConfig as _;

const FIELD_MANAGER_NAME: &str = "kube-node-label-global-ips";

const MAX_RETRIES: u32 = 10;

const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug, Parser)]
enum Command {
    Once,
    Repeat { every: String },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let command = Command::parse();

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match run(command).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let causes = error
                .chain()
                .skip(1)
                .map(|error| format!("{error}"))
                .collect::<Vec<_>>();
            tracing::error!(?causes, "{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(command: Command) -> miette::Result<()> {
    match command {
        Command::Once => {
            update_ips().await?;
        }
        Command::Repeat { every } => {
            let every = humantime::parse_duration(&every)
                .into_diagnostic()
                .wrap_err_with(|| format!("invalid value for repeat: {every:?}"))?;

            update_ips_repeatedly(every).await?;
        }
    }

    Ok(())
}

async fn update_ips() -> miette::Result<()> {
    let node_name = std::env::var("KUBE_NODE_NAME")
        .into_diagnostic()
        .wrap_err("$KUBE_NODE_NAME must be set")?;

    let global_ips = tokio::task::spawn_blocking(|| {
        let mut global_ips = HashSet::new();

        let ifaces = network_interface::NetworkInterface::show()
            .into_diagnostic()
            .wrap_err("failed to get network interfaces")?;

        for iface in ifaces {
            let iface_global_ips = iface
                .addr
                .iter()
                .filter_map(|addr| {
                    let ip = addr.ip();
                    if ip_rfc::global(&ip) { Some(ip) } else { None }
                })
                .collect::<Vec<_>>();
            if iface_global_ips.is_empty() {
                tracing::info!(iface = iface.name, "interface has no global IPs");
            } else {
                tracing::info!(iface = iface.name, ips = ?iface_global_ips, "found global IPs from interface");
            }

            global_ips.extend(iface_global_ips);
        }

        if global_ips.is_empty() {
            tracing::warn!("no global IPs found!");
        }

        Ok::<_, miette::Error>(global_ips)
    }).await.into_diagnostic()??;
    let global_ips = global_ips.join_with(",").to_string();

    let kube = kube::Client::try_default()
        .await
        .into_diagnostic()
        .wrap_err("failed to build Kubernetes client")?;
    let nodes = kube::Api::<k8s_openapi::api::core::v1::Node>::all(kube);

    let mut retries = MAX_RETRIES;
    loop {
        let mut node_entry = nodes
            .entry(&node_name)
            .await
            .into_diagnostic()
            .wrap_err("failed to get Kubernetes node resource")?;
        let kube::api::entry::Entry::Occupied(node_entry) = &mut node_entry else {
            miette::bail!("node not found: {node_name:?}");
        };

        node_entry
            .get_mut()
            .metadata
            .annotations
            .get_or_insert_default()
            .insert(
                "external-dns.alpha.kubernetes.io/target".to_string(),
                global_ips.clone(),
            );

        let result = node_entry
            .commit(&kube::api::PostParams {
                dry_run: false,
                field_manager: Some(FIELD_MANAGER_NAME.to_string()),
            })
            .await;
        match result {
            Ok(()) => {
                tracing::info!("updated node label with global IPs");
                return Ok(());
            }
            Err(error) => {
                let Some(remaining_retries) = retries.checked_sub(1) else {
                    return Err(error)
                        .into_diagnostic()
                        .wrap_err("failed to update node");
                };
                retries = remaining_retries;

                tracing::warn!("request failed, retrying: {error:?}");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

async fn update_ips_repeatedly(every: std::time::Duration) -> miette::Result<()> {
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        tracing::info!("updating IPs");

        update_ips().await?;

        tokio::select! {
            result = &mut ctrl_c => {
                result.into_diagnostic().wrap_err("Ctrl-C handler failed")?;
                tracing::info!("received Ctrl-C signal, shutting down...");
                return Ok(());
            }
            _ = tokio::time::sleep(every) => {
                // Ready to update IPs again
            }
        }
    }
}
