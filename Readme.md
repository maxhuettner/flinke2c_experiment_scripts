# Network Sim Experiments

Run all commands from the repository root.

## Prerequisites

Install the Python CLI:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

You also need these tools installed locally:

```bash
terraform
ansible-playbook
ssh
rsync
docker
```

## One-Time Preparation

1. Make sure your SSH key is available locally.
2. Export the variables used by the CLI:

```bash
export SSH_KEY_PATH=~/.ssh/id_ed25519
export ANSIBLE_SSH_KEY_PATH=~/.ssh/id_ed25519
export ANSIBLE_CLOUD_SSH_USER=ubuntu
export AWS_REGION=eu-central-1
```

3. If you use on-prem nodes, verify [exp_management/ansible/inventory/onprem.yml](/Users/max/dev/work/network_sim/exp_management/ansible/inventory/onprem.yml) contains the correct hosts.

## Command Order

The workflow is always:

```bash
sim setup -f <topology.json>
sim experiment -f <topology.json> -e <experiments.yml> -o results/<name>
```

If you run another batch on the same machines and the source data is already on the remote nodes, use:

```bash
sim experiment -f <topology.json> -e <experiments.yml> -o results/<name> --skip-data-upload
```

If the topology contains cloud nodes, tear them down afterwards:

```bash
sim destroy -f <topology.json>
```

## Ready-To-Run Examples

### On-Prem

```bash
sim setup -f config/topologies/on-prem.json
sim experiment -f config/topologies/on-prem.json -e exp_management/experiments.on_prem.yml -o results/on_prem
```

### Edge-to-Cloud

```bash
sim setup -f config/topologies/edge-to-cloud.json
sim experiment -f config/topologies/edge-to-cloud.json -e exp_management/experiments.e2c.yml -o results/e2c
sim destroy -f config/topologies/edge-to-cloud.json
```

### Cloud

```bash
sim setup -f config/topologies/cloud.json
sim experiment -f config/topologies/cloud.json -e exp_management/experiments.cloud.yml -o results/cloud
sim destroy -f config/topologies/cloud.json
```

### Edge-Heavy

```bash
sim setup -f config/topologies/edge-heavy.json
sim experiment -f config/topologies/edge-heavy.json -e exp_management/experiments.edge_heavy.yml -o results/edge_heavy
sim destroy -f config/topologies/edge-heavy.json
```

## Notes

- `sim setup` provisions cloud machines when needed, generates the Ansible inventory, and runs Ansible.
- `sim experiment` uploads the files from `exp_management/source_data/` before the first run unless you pass `--skip-data-upload`.
- Logs are downloaded into `results/<name>/<experiment-name>/`.
- Query-specific settings such as `num_task_slots` are read from [exp_management/configs/flink/query_config.yml](/Users/max/dev/work/network_sim/exp_management/configs/flink/query_config.yml) and can be overridden in the experiment YAML files.
