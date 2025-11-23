use std::collections::HashSet;

use clap::Parser;
use joinery::{Joinable as _, JoinableIterator as _};
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
    let node_annotation_ips = std::env::var("KUBE_NODE_ANNOTATION_IPS").ok();
    let node_annotation_node_name = std::env::var("KUBE_NODE_ANNOTATION_NODE_NAME").ok();
    let node_annotation_target = std::env::var("KUBE_NODE_ANNOTATION_TARGET").ok();
    let node_annotation_coordinates = std::env::var("KUBE_NODE_ANNOTATION_COORDINATES").ok();
    let node_annotation_target_num_neighbors: usize =
        std::env::var("KUBE_NODE_ANNOTATION_TARGET_NUM_NEIGHBORS")
            .ok()
            .map(|num_neighbors| num_neighbors.parse())
            .transpose()
            .into_diagnostic()
            .wrap_err("invalid value for $KUBE_NODE_ANNOTATION_TARGET_NUM_NEIGHBORS")?
            .unwrap_or(0);
    let node_annotation_target_neighbor_label_selector =
        std::env::var("KUBE_NODE_ANNOTATION_TARGET_NEIGHBOR_LABEL_SELECTOR").ok();
    miette::ensure!(
        node_annotation_ips.is_some()
            || node_annotation_node_name.is_some()
            || node_annotation_target.is_some(),
        "no annotations are enabled"
    );

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

        if let Some(annotation) = &node_annotation_ips {
            let global_ips = get_ips().await?;
            let global_ip_list = global_ips.join_with(",").to_string();

            if set_annotation(node_entry, &annotation, &global_ip_list) {
                tracing::info!(
                    node = node_name,
                    global_ip_list,
                    annotation,
                    "updated node annotation with IPs"
                );
            } else {
                tracing::info!(
                    node = node_name,
                    global_ip_list,
                    annotation,
                    "node IP annotation already up-to-date"
                );
            }
        }

        if let Some(annotation) = &node_annotation_node_name {
            if set_annotation(node_entry, &annotation, &node_name) {
                tracing::info!(
                    node = node_name,
                    name = node_name,
                    annotation,
                    "updated node annotation with node name"
                );
            } else {
                tracing::info!(
                    node = node_name,
                    name = node_name,
                    annotation,
                    "node name annotation already up-to-date"
                );
            }
        }

        if let Some(target_annotation) = &node_annotation_target {
            let global_ips = get_ips().await?;
            let mut targets = vec![global_ips.iter().join_with(",").to_string()];

            if node_annotation_target_num_neighbors > 0 {
                // Find the nearest neighbor nodes to add to the target too

                let ips_annotation = node_annotation_ips
                    .as_ref()
                    .wrap_err("$KUBE_NODE_ANNOTATION_IPS must be set with $NODE_ANNOTATION_TARGET_NUM_NEIGHBORS")?;

                let coordinates = node_annotation_coordinates.as_ref().and_then(|annotation| {
                    let coordinates = node_coordinates(node_entry.get(), annotation)
                        .inspect_err(|err| {
                            tracing::warn!(
                                node = node_name,
                                annotation,
                                "invalid coordinates on node: {err:?}"
                            );
                        })
                        .ok()??;
                    Some((annotation, coordinates))
                });
                if let Some((coordinates_annotation, node_coords)) = coordinates {
                    let mut node_list_params = kube::api::ListParams::default();
                    if let Some(label_selector) = &node_annotation_target_neighbor_label_selector {
                        // Filter with label selector, if provided
                        node_list_params = node_list_params.labels(&label_selector);
                    }

                    let node_list = nodes
                        .list(&node_list_params)
                        .await
                        .into_diagnostic()
                        .wrap_err("failed to list nodes")?;

                    // Get other nodes with coordinates and IPs set
                    let mut neighbors = node_list
                        .items
                        .into_iter()
                        .filter(|neighbor| {
                            // Exclude the current node
                            neighbor.metadata.name.as_ref() != Some(&node_name)
                        })
                        .filter_map(|neighbor| {
                            // Consider only nodes with both IP and coordinate
                            // annotations
                            let neighbor_ips = neighbor
                                .metadata
                                .annotations
                                .as_ref()
                                .and_then(|annotations| annotations.get(ips_annotation))?
                                .clone();
                            let neighbor_coords =
                                node_coordinates(&neighbor, &coordinates_annotation)
                                    .ok()
                                    .flatten()?;
                            let dist = distance_between_coordinates(node_coords, neighbor_coords);

                            Some((neighbor_ips, neighbor_coords, dist))
                        })
                        .collect::<Vec<_>>();

                    // Sort nodes by distance from the current node
                    neighbors.sort_by_cached_key(|(_, _, dist)| {
                        ordered_float::NotNan::new(*dist).unwrap()
                    });

                    if neighbors.len() > 0 {
                        tracing::info!(
                            ?global_ips,
                            ?neighbors,
                            node_annotation_target_num_neighbors,
                            "found extra neighbors to add to target"
                        )
                    } else {
                        tracing::info!(
                            ?global_ips,
                            node_annotation_target_num_neighbors,
                            "no extra neighbors found to add to target"
                        );
                    }

                    // Take only the desired number of neighbors
                    neighbors.truncate(node_annotation_target_num_neighbors);
                    let neighbor_ips = neighbors
                        .into_iter()
                        .map(|(neighbor_ips, _, _)| neighbor_ips)
                        .take(node_annotation_target_num_neighbors)
                        .collect::<Vec<_>>();
                    targets.extend(neighbor_ips);
                } else if let Some(annotation) = &node_annotation_coordinates {
                    tracing::warn!(
                        node_annotation_target_num_neighbors,
                        annotation,
                        "coordinates annotation not set for node, ignoring neighbors"
                    );
                } else {
                    miette::bail!(
                        "configured to set neighbors, but $KUBE_NODE_ANNOTATION_TARGET_NUM_NEIGHBORS must be > 0"
                    );
                }
            }

            let target_list = targets.join_with(",").to_string();
            if set_annotation(node_entry, &target_annotation, &target_list) {
                tracing::info!(
                    node = node_name,
                    target_list,
                    annotation = target_annotation,
                    "updated node annotation with targets"
                );
            } else {
                tracing::info!(
                    node = node_name,
                    target_list,
                    annotation = target_annotation,
                    "node target annotation already up-to-date"
                )
            }
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

fn node_coordinates(node: &Node, annotation: &str) -> miette::Result<Option<Coordinates>> {
    let Some(annotations) = node.metadata.annotations.as_ref() else {
        return Ok(None);
    };

    let Some(coordinates) = annotations.get(annotation) else {
        return Ok(None);
    };

    let coordinates: Coordinates = coordinates.parse()?;
    Ok(Some(coordinates))
}

const EARTH_RADIUS: f32 = 6371.0;

fn distance_between_coordinates(a: Coordinates, b: Coordinates) -> f32 {
    let delta_lat = (b.lat_rad() - a.lat_rad()).abs();
    let delta_lon = (b.lon_rad() - a.lon_rad()).abs();
    let hav_delta_lat = (1.0 - f32::cos(delta_lat)) / 2.0;
    let hav_delta_lon = (1.0 - f32::cos(delta_lon)) / 2.0;
    let hav_great_circle_dist =
        hav_delta_lat + (f32::cos(a.lat_rad()) * f32::cos(b.lat_rad()) * hav_delta_lon);
    let great_circle_dist = 2.0 * f32::asin(f32::sqrt(hav_great_circle_dist));
    great_circle_dist * EARTH_RADIUS
}

#[derive(Debug, Clone, Copy)]
struct Coordinates {
    latitude: f32,
    longitude: f32,
}

impl Coordinates {
    fn lat_rad(&self) -> f32 {
        self.latitude.to_radians()
    }

    fn lon_rad(&self) -> f32 {
        self.longitude.to_radians()
    }
}

impl std::str::FromStr for Coordinates {
    type Err = miette::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let coordinates = s.split_once(",").and_then(|(lat, lon)| {
            let lat: f32 = lat.parse().ok()?;
            let lon: f32 = lon.parse().ok()?;
            Some((lat, lon))
        });
        let Some((latitude, longitude)) = coordinates else {
            miette::bail!(
                "invalid coordinates: expected a string of the form 'latitude,longitude', got {s:?}"
            );
        };

        Ok(Self {
            latitude,
            longitude,
        })
    }
}
