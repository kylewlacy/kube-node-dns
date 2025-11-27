use std::collections::{BTreeSet, HashMap, HashSet};

use clap::Parser;
use futures::StreamExt as _;
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
    Repeat {
        every: String,
    },
    PublishDns {
        #[arg(long = "domain", num_args(1..))]
        domains: Vec<String>,
        #[arg(long)]
        node_label_selector: Option<String>,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let command = Command::parse();

    tracing_subscriber::fmt()
        .compact()
        .without_time()
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
        Command::PublishDns {
            domains,
            node_label_selector,
        } => {
            run_dns_publisher(&domains, node_label_selector.as_deref()).await?;
        }
    }

    Ok(())
}

async fn get_ips() -> miette::Result<BTreeSet<std::net::IpAddr>> {
    let global_ips = tokio::task::spawn_blocking(|| {
        let mut global_ips = BTreeSet::new();

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
    miette::ensure!(
        node_annotation_ips.is_some() || node_annotation_node_name.is_some(),
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
    let mut shutdown_signal = shutdown_signal();

    tracing::info!("updating IPs every {}", humantime::format_duration(every));

    loop {
        update_nodes().await?;

        tokio::select! {
            result = &mut shutdown_signal => {
                result.into_diagnostic().wrap_err("error receiving shutdown signal")?.wrap_err("error handling shutdown signal")?;
                tracing::info!("received shutdown signal, shutting down...");
                return Ok(());
            }
            _ = tokio::time::sleep(every) => {
                tracing::info!("updating IPs after {}", humantime::format_duration(every));

                // Ready to update IPs again
            }
        }
    }
}

async fn run_dns_publisher(domains: &[String], node_selector: Option<&str>) -> miette::Result<()> {
    let aws_config = aws_config::load_from_env().await;
    let route53 = aws_sdk_route53::Client::new(&aws_config);

    let mut public_hosted_zones = vec![];
    let mut hosted_zone_stream = route53.list_hosted_zones().into_paginator().send();
    while let Some(hosted_zones_page) = hosted_zone_stream.next().await {
        let hosted_zones_page = hosted_zones_page.into_diagnostic()?;
        public_hosted_zones.extend(hosted_zones_page.hosted_zones.into_iter().filter(
            |hosted_zone| {
                hosted_zone
                    .config
                    .as_ref()
                    .is_none_or(|config| !config.private_zone)
            },
        ));
    }

    let mut domains_by_hosted_zone_id = HashMap::<String, Vec<_>>::new();
    for domain in domains {
        let hosted_zone_domain = find_hosted_zone_for_domain(&public_hosted_zones, domain)?;
        domains_by_hosted_zone_id
            .entry(hosted_zone_domain.hosted_zone_id.clone())
            .or_default()
            .push(hosted_zone_domain);
    }

    let kube = kube::Client::try_default()
        .await
        .into_diagnostic()
        .wrap_err("failed to build Kubernetes client")?;
    let nodes = kube::Api::<Node>::all(kube);

    let mut state = DnsPublisher {
        domains_by_hosted_zone_id,
        nodes: HashMap::new(),
    };
    let mut pending_changes = false;

    let mut node_watch_config = kube::runtime::watcher::Config::default();
    if let Some(node_selector) = node_selector {
        node_watch_config = node_watch_config.labels(node_selector);
    }

    let mut shutdown_signal = shutdown_signal();
    let node_stream = kube::runtime::watcher(nodes, node_watch_config);
    let mut node_stream = std::pin::pin!(node_stream);

    loop {
        let event = tokio::select! {
            result = &mut shutdown_signal => {
                result.into_diagnostic().wrap_err("error receiving shutdown signal")?.wrap_err("error handling shutdown signal")?;
                tracing::info!("received shutdown signal, shutting down...");
                return Ok(());
            }
            event = node_stream.next() => event,
        };
        let event = match event {
            Some(Ok(event)) => event,
            Some(Err(error)) => {
                tracing::error!("error in node watch stream: {error:?}");
                continue;
            }
            None => {
                // Stream ended
                break;
            }
        };

        let is_current = match event {
            kube::runtime::watcher::Event::Apply(_)
            | kube::runtime::watcher::Event::Delete(_)
            | kube::runtime::watcher::Event::InitDone => true,
            kube::runtime::watcher::Event::Init | kube::runtime::watcher::Event::InitApply(_) => {
                false
            }
        };
        let result = state.handle_event(&event);
        match result {
            Ok(true) => {
                // Changes made - ensure we publish when we're ready
                pending_changes = true;
            }
            Ok(false) => {
                // No changes made
            }
            Err(error) => {
                // Error when handling event
                tracing::error!("error when handling node event: {error:?}");
            }
        };

        let should_publish = is_current && pending_changes;
        if should_publish {
            // The watcher is up-to-date with current events and we have
            // some pending changes, so publish them

            for (hosted_zone_id, domains) in &state.domains_by_hosted_zone_id {
                publish_dns_records_for_nodes_in_route53(
                    &route53,
                    &hosted_zone_id,
                    &domains,
                    state.nodes.values(),
                )
                .await?;
            }

            tracing::info!(
                domains = ?state
                    .domains_by_hosted_zone_id
                    .values()
                    .flatten()
                    .map(|domain| &domain.domain)
                    .collect::<Vec<_>>(),
                "finished publishing DNS records to Route53"
            );

            pending_changes = false;
        }
    }

    tracing::info!("watch stream ended");

    Ok(())
}

struct DnsPublisher {
    nodes: HashMap<String, NodePublishState>,
    domains_by_hosted_zone_id: HashMap<String, Vec<HostedZoneDomain>>,
}

impl DnsPublisher {
    fn handle_event(
        &mut self,
        event: &kube::runtime::watcher::Event<Node>,
    ) -> miette::Result<bool> {
        match event {
            kube::runtime::watcher::Event::Apply(node)
            | kube::runtime::watcher::Event::InitApply(node) => {
                let name = node
                    .metadata
                    .name
                    .as_ref()
                    .wrap_err("received 'apply' node event without a name")?;

                let node_state = NodeState::from_node(node)?;
                let node_state = match node_state {
                    NodeState::Publish(node_state) => Some(node_state),
                    NodeState::NoIps => {
                        tracing::info!(node = name, "node does not have annotation with IPs");
                        None
                    }
                    NodeState::Unschedulable => {
                        tracing::info!(node = name, "node is marked unschedulable");
                        None
                    }
                    NodeState::NotReady { ready_status } => {
                        tracing::info!(node = name, ?ready_status, "node is not ready");
                        None
                    }
                };

                match self.nodes.entry(name.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if let Some(node_state) = node_state {
                            if *entry.get() == node_state {
                                tracing::debug!(
                                    node = name,
                                    "skipping node update event, node state did not change"
                                );
                                Ok(false)
                            } else {
                                tracing::debug!(node = name, "inserted updated node state");
                                entry.insert(node_state);
                                Ok(true)
                            }
                        } else {
                            tracing::debug!(
                                node = name,
                                "removing node state, new node state is None"
                            );
                            entry.remove();
                            Ok(true)
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        if let Some(node_state) = node_state {
                            tracing::debug!(node = name, "inserted new node state");
                            entry.insert(node_state);
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                }
            }
            kube::runtime::watcher::Event::Delete(node) => {
                let name = node
                    .metadata
                    .name
                    .clone()
                    .wrap_err("received 'delete' node event without a name")?;
                let removed = self.nodes.remove(&name);
                if removed.is_some() {
                    tracing::debug!(node = name, "removing node state, node deleted");
                }
                Ok(removed.is_some())
            }
            kube::runtime::watcher::Event::Init | kube::runtime::watcher::Event::InitDone => {
                // No op
                Ok(false)
            }
        }
    }
}

enum NodeState {
    Publish(NodePublishState),
    NoIps,
    Unschedulable,
    NotReady { ready_status: Option<String> },
}

impl NodeState {
    fn from_node(node: &Node) -> miette::Result<Self> {
        let name = node
            .metadata
            .name
            .as_ref()
            .wrap_err("node metadata is missing 'name' field")?;

        let is_unschedulable = node
            .spec
            .as_ref()
            .and_then(|spec| spec.unschedulable)
            .unwrap_or(false);
        if is_unschedulable {
            return Ok(Self::Unschedulable);
        }

        let node_conditions = node
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_deref())
            .unwrap_or_default();
        let ready_status = node_conditions.iter().find_map(|condition| {
            if condition.type_ == "Ready" {
                Some(&condition.status)
            } else {
                None
            }
        });
        let is_ready = ready_status.is_none_or(|status| status == "True");
        if !is_ready {
            return Ok(Self::NotReady {
                ready_status: ready_status.cloned(),
            });
        }

        let ips = node
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("external-dns.alpha.kubernetes.io/target"));
        let ips = ips.iter().flat_map(|ips| ips.split(','));

        let mut ipv4s = vec![];
        let mut ipv6s = vec![];
        for ip in ips {
            let ip: std::net::IpAddr = ip
                .parse()
                .into_diagnostic()
                .wrap_err_with(|| format!("invalid IP for node {name}: {ip}"))?;
            match ip {
                std::net::IpAddr::V4(ip) => {
                    ipv4s.push(ip);
                }
                std::net::IpAddr::V6(ip) => {
                    ipv6s.push(ip);
                }
            }
        }

        if ipv4s.is_empty() && ipv6s.is_empty() {
            return Ok(Self::NoIps);
        }

        let coordinates: Option<Coordinates> = node
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| {
                annotations.get("external-dns.alpha.kubernetes.io/aws-geoproximity-coordinates")
            })
            .map(|coordinates| coordinates.parse())
            .transpose()?;

        Ok(Self::Publish(NodePublishState {
            name: name.clone(),
            ipv4s,
            ipv6s,
            coordinates,
        }))
    }
}

#[derive(Debug, PartialEq)]
struct NodePublishState {
    name: String,
    ipv4s: Vec<std::net::Ipv4Addr>,
    ipv6s: Vec<std::net::Ipv6Addr>,
    coordinates: Option<Coordinates>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Coordinates {
    latitude: String,
    longitude: String,
}

impl std::fmt::Display for Coordinates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.latitude, self.longitude)
    }
}

impl std::str::FromStr for Coordinates {
    type Err = miette::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split_once(',')
            .and_then(|(lat, lon)| {
                Some(Self {
                    latitude: lat.to_string(),
                    longitude: lon.to_string(),
                })
            })
            .wrap_err_with(|| {
                format!(
                    "invalid coordinates: expected string of the format '<lat>,<lon>', got: {s:?}"
                )
            })
    }
}

impl TryFrom<Coordinates> for aws_sdk_route53::types::Coordinates {
    type Error = aws_sdk_route53::error::BuildError;

    fn try_from(value: Coordinates) -> Result<Self, Self::Error> {
        Self::builder()
            .latitude(value.latitude)
            .longitude(value.longitude)
            .build()
    }
}

#[derive(Debug, Clone)]
struct HostedZoneDomain {
    domain: String,
    hosted_zone_name: String,
    hosted_zone_id: String,
    record_name: String,
}

fn find_hosted_zone_for_domain(
    hosted_zones: &[aws_sdk_route53::types::HostedZone],
    domain: &str,
) -> miette::Result<HostedZoneDomain> {
    let hosted_zone_domains = hosted_zones.iter().filter_map(|hosted_zone| {
        let hosted_zone_domain = remove_fqdn_trailing_dot(&hosted_zone.name);
        if hosted_zone_domain == domain {
            Some(HostedZoneDomain {
                domain: domain.to_string(),
                hosted_zone_name: hosted_zone.name.clone(),
                hosted_zone_id: hosted_zone.id.clone(),
                record_name: "@".to_string(),
            })
        } else if let Some((rest, "")) = domain.rsplit_once(&hosted_zone_domain)
            && let Some((record_name, "")) = rest.rsplit_once('.')
        {
            Some(HostedZoneDomain {
                domain: domain.to_string(),
                hosted_zone_name: hosted_zone.name.clone(),
                hosted_zone_id: hosted_zone.id.clone(),
                record_name: record_name.to_string(),
            })
        } else {
            None
        }
    });

    let hosted_zone_domain = hosted_zone_domains
        .max_by_key(|hosted_zone_domain| hosted_zone_domain.hosted_zone_name.len())
        .wrap_err_with(|| format!("no hosted zone found for domain: {domain}"))?;
    Ok(hosted_zone_domain)
}

async fn publish_dns_records_for_nodes_in_route53(
    route53: &aws_sdk_route53::Client,
    hosted_zone_id: &str,
    records: &[HostedZoneDomain],
    nodes: impl Iterator<Item = &NodePublishState> + Clone,
) -> miette::Result<()> {
    let record_names = records
        .iter()
        .map(|record| &*record.domain)
        .collect::<HashSet<_>>();

    let mut target_record_sets = HashMap::<ResourceRecordSetKey, ResourceRecordSetValue>::new();
    for record in records {
        for node in nodes.clone() {
            let key = ResourceRecordSetKey {
                domain: record.domain.clone(),
                coordinates: node.coordinates.clone(),
            };
            target_record_sets
                .entry(key.clone())
                .or_default()
                .ipv4s
                .extend(node.ipv4s.iter().copied());
            target_record_sets
                .entry(key)
                .or_default()
                .ipv6s
                .extend(node.ipv6s.iter().copied());
        }
    }

    let mut current_record_sets =
        list_route53_resource_record_sets(route53, hosted_zone_id).await?;
    current_record_sets.retain(|record_set| {
        // Filter to only existing A / AAAA records
        let name = match record_set.name.rsplit_once('.') {
            Some((name, "")) => name,
            _ => &record_set.name,
        };
        let is_a_or_aaaa = matches!(
            record_set.r#type,
            aws_sdk_route53::types::RrType::A | aws_sdk_route53::types::RrType::Aaaa
        );
        is_a_or_aaaa && record_names.contains(name)
    });

    let mut new_record_sets = vec![];
    for (key, value) in target_record_sets {
        let set_identifier = if let Some(coordinates) = &key.coordinates {
            Some(format!("geoproximity-coords:{coordinates}"))
        } else {
            None
        };
        let geo_proximity_location = if let Some(coordinates) = &key.coordinates {
            let coordinates = coordinates.clone().try_into().into_diagnostic()?;
            Some(
                aws_sdk_route53::types::GeoProximityLocation::builder()
                    .coordinates(coordinates)
                    .build(),
            )
        } else {
            None
        };

        if !value.ipv4s.is_empty() {
            let resource_records = value
                .ipv4s
                .iter()
                .map(|ip| {
                    aws_sdk_route53::types::ResourceRecord::builder()
                        .value(ip.to_string())
                        .build()
                        .into_diagnostic()
                })
                .collect::<miette::Result<_>>()?;
            let new_record_set = aws_sdk_route53::types::ResourceRecordSet::builder()
                .name(&key.domain)
                .r#type(aws_sdk_route53::types::RrType::A)
                .set_resource_records(Some(resource_records))
                .set_set_identifier(set_identifier.clone())
                .set_geo_proximity_location(geo_proximity_location.clone())
                .ttl(300)
                .build()
                .into_diagnostic()?;
            new_record_sets.push(new_record_set)
        }

        if !value.ipv6s.is_empty() {
            let resource_records = value
                .ipv6s
                .iter()
                .map(|ip| {
                    aws_sdk_route53::types::ResourceRecord::builder()
                        .value(ip.to_string())
                        .build()
                        .into_diagnostic()
                })
                .collect::<miette::Result<Vec<_>>>()?;
            let new_record_set = aws_sdk_route53::types::ResourceRecordSet::builder()
                .name(&key.domain)
                .r#type(aws_sdk_route53::types::RrType::Aaaa)
                .set_resource_records(Some(resource_records))
                .set_set_identifier(set_identifier)
                .set_geo_proximity_location(geo_proximity_location)
                .ttl(300)
                .build()
                .into_diagnostic()?;
            new_record_sets.push(new_record_set)
        }
    }

    let changes = current_record_sets
        .into_iter()
        .map(|record_set| {
            aws_sdk_route53::types::Change::builder()
                .action(aws_sdk_route53::types::ChangeAction::Delete)
                .resource_record_set(record_set)
                .build()
                .into_diagnostic()
        })
        .chain(new_record_sets.into_iter().map(|record_set| {
            aws_sdk_route53::types::Change::builder()
                .action(aws_sdk_route53::types::ChangeAction::Create)
                .resource_record_set(record_set)
                .build()
                .into_diagnostic()
        }))
        .collect::<miette::Result<Vec<_>>>()?;
    let change_batch = aws_sdk_route53::types::ChangeBatch::builder()
        .set_changes(Some(changes))
        .build()
        .into_diagnostic()?;

    tracing::debug!("sending route53 change batch:\n{change_batch:#?}");

    route53
        .change_resource_record_sets()
        .hosted_zone_id(hosted_zone_id)
        .change_batch(change_batch)
        .send()
        .await
        .into_diagnostic()?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResourceRecordSetKey {
    domain: String,
    coordinates: Option<Coordinates>,
}

#[derive(Debug, Default)]
struct ResourceRecordSetValue {
    ipv4s: BTreeSet<std::net::Ipv4Addr>,
    ipv6s: BTreeSet<std::net::Ipv6Addr>,
}

async fn list_route53_resource_record_sets(
    route53: &aws_sdk_route53::Client,
    hosted_zone_id: &str,
) -> miette::Result<Vec<aws_sdk_route53::types::ResourceRecordSet>> {
    let mut all_record_sets = vec![];

    let mut next_record_name = None;
    let mut next_record_type = None;
    let mut next_record_identifier = None;
    loop {
        let record_sets = route53
            .list_resource_record_sets()
            .hosted_zone_id(hosted_zone_id)
            .set_start_record_name(next_record_name)
            .set_start_record_type(next_record_type)
            .set_start_record_identifier(next_record_identifier)
            .send()
            .await
            .into_diagnostic()?;

        all_record_sets.extend(record_sets.resource_record_sets);

        if !record_sets.is_truncated {
            break;
        }

        next_record_name = record_sets.next_record_name;
        next_record_type = record_sets.next_record_type;
        next_record_identifier = record_sets.next_record_identifier;

        miette::ensure!(
            next_record_name.is_some()
                || next_record_type.is_some()
                || next_record_identifier.is_some(),
            "Route53 record set result was truncated, but we have no next value"
        );
    }

    Ok(all_record_sets)
}

fn remove_fqdn_trailing_dot(domain: &str) -> &str {
    match domain.rsplit_once('.') {
        Some((domain, "")) => domain,
        _ => domain,
    }
}

fn shutdown_signal() -> tokio::sync::oneshot::Receiver<miette::Result<()>> {
    let (tx, rx) = tokio::sync::oneshot::channel::<miette::Result<()>>();

    tokio::task::spawn(async move {
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

        let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        let mut sigterm = match sigterm {
            Ok(sigterm) => sigterm,
            Err(error) => {
                let _ = tx.send(
                    Err(error)
                        .into_diagnostic()
                        .wrap_err("failed to install SIGTERM handler"),
                );
                return;
            }
        };

        tokio::select! {
            result = &mut ctrl_c => {
                let _ = tx.send(result.into_diagnostic().wrap_err("Ctrl-C handler failed"));
            }
            Some(()) = sigterm.recv() => {
                let _ = tx.send(Ok(()));
            }
        }
    });

    rx
}
