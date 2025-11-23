use std::collections::HashSet;

use clap::Parser;
use joinery::Joinable as _;
use k8s_openapi::api::core::v1::Node;
use miette::{Context as _, IntoDiagnostic as _};
use network_interface::NetworkInterfaceConfig as _;

const FIELD_MANAGER_NAME: &str = "kube-node-annotate-ips";

const MAX_RETRIES: u32 = 10;

const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug, Parser)]
enum Command {
    ListIps,
    Once,
    Repeat { every: String },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let command = Command::parse();

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
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
        Command::ListIps => {
            let ips = get_ips().await?;
            for ip in ips {
                println!("{ip}");
            }
        }
        Command::Once => {
            update_nodes().await?;
        }
        Command::Repeat { every } => {
            let every = humantime::parse_duration(&every)
                .into_diagnostic()
                .wrap_err_with(|| format!("invalid value for repeat: {every:?}"))?;

            update_nodes_repeatedly(every).await?;
        }
    }

    Ok(())
}

async fn get_ips() -> miette::Result<HashSet<std::net::IpAddr>> {
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
                tracing::debug!(iface = iface.name, "interface has no global IPs");
            } else {
                tracing::debug!(iface = iface.name, ips = ?iface_global_ips, "found global IPs from interface");
            }

            global_ips.extend(iface_global_ips);
        }

        if global_ips.is_empty() {
            tracing::warn!("no global IPs found, but proceeding anyway!");
        }

        Ok::<_, miette::Error>(global_ips)
    }).await.into_diagnostic()??;

    Ok(global_ips)
}

async fn update_nodes() -> miette::Result<()> {
    let node_name = std::env::var("KUBE_NODE_NAME")
        .into_diagnostic()
        .wrap_err("$KUBE_NODE_NAME must be set")?;
    let node_annotation_ips = std::env::var("KUBE_NODE_ANNOTATION_IPS")
        .into_diagnostic()
        .wrap_err("$KUBE_NODE_ANNOTATION_IPS must be set")?;

    let kube = kube::Client::try_default()
        .await
        .into_diagnostic()
        .wrap_err("failed to build Kubernetes client")?;
    let nodes = kube::Api::<Node>::all(kube);

    // Retry in a loop in case we get a write conflict from the Kubernetes API
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

        let global_ips = get_ips().await?;
        let global_ip_list = global_ips.join_with(",").to_string();

        if set_annotation(node_entry, &node_annotation_ips, &global_ip_list) {
            tracing::info!(
                node = node_name,
                global_ip_list,
                annotation = node_annotation_ips,
                "updated node annotation with IPs"
            );
        } else {
            tracing::info!(
                node = node_name,
                global_ip_list,
                annotation = node_annotation_ips,
                "node IP annotation already up-to-date"
            );
        }

        let result = node_entry
            .commit(&kube::api::PostParams {
                dry_run: false,
                field_manager: Some(FIELD_MANAGER_NAME.to_string()),
            })
            .await;
        match result {
            Ok(()) => {
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

fn set_annotation(
    entry: &mut kube::api::entry::OccupiedEntry<Node>,
    annotation: &str,
    value: &str,
) -> bool {
    let current_value = entry
        .get()
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(annotation))
        .map(|value| &**value);
    if current_value == Some(value) {
        return false;
    }

    entry
        .get_mut()
        .metadata
        .annotations
        .get_or_insert_default()
        .insert(annotation.to_string(), value.to_string());
    true
}

async fn update_nodes_repeatedly(every: std::time::Duration) -> miette::Result<()> {
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .into_diagnostic()
        .wrap_err("failed to install SIGTERM handler")?;

    tracing::info!("updating IPs every {}", humantime::format_duration(every));

    loop {
        update_nodes().await?;

        tokio::select! {
            result = &mut ctrl_c => {
                result.into_diagnostic().wrap_err("Ctrl-C handler failed")?;
                tracing::info!("received Ctrl-C signal, shutting down...");
                return Ok(());
            }
            Some(()) = sigterm.recv() => {
                tracing::info!("received SIGTERM signal, shutting down...");
                return Ok(());
            }
            _ = tokio::time::sleep(every) => {
                tracing::info!("updating IPs after {}", humantime::format_duration(every));

                // Ready to update IPs again
            }
        }
    }
}
