# Experiment Ansible Automation

This directory provides the Ansible layer that the `exp_management` CLI can
invoke after Terraform finishes provisioning AWS resources (and after the CLI
discovers the already existing on‑prem hosts). The CLI is responsible for
rendering the inventory based on the GraphML files:

1. **Cloud vs On‑prem** – every node must specify `node_class` (`cloud` or
   `onprem`) so roles can branch on the correct dependency set.
2. **WireGuard metadata** – if a node participates in the WireGuard overlay, the
   CLI should place a `wireguard` structure in host vars that includes the
   private key, interface address, listen port, and generated peer list. The
   peer list is derived from the GraphML edges.
3. **Connection details** – set `ansible_host`, `ansible_user`, `ansible_ssh_private_key_file`
   (or `ansible_password`) so Ansible can connect to each host.

```
exp_management/ansible/
├── ansible.cfg
├── inventory/
│   ├── hosts.example.yml        # shape the CLI should mimic
│   └── generated_hosts.yml      # written by the CLI (gitignored)
├── group_vars/
│   └── all.yml
├── playbooks/
│   └── site.yml
└── roles/
    ├── common/
    │   └── tasks/main.yml
    └── wireguard/
        ├── defaults/main.yml
        ├── handlers/main.yml
        ├── tasks/main.yml
        └── templates/wg.conf.j2
```

## Inventory contract

The CLI automatically writes `inventory/generated_hosts.yml` during `exp_management
--command-type setup ...`. The generated file merges the static on-prem content
(`inventory/onprem.yml`) with the AWS instances that Terraform just created. If
you ever need to craft it manually (for testing), follow the example below:

```yaml
all:
  children:
    cloud:
      hosts:
        ec2-node-1:
          ansible_host: 3.80.1.23
          ansible_user: ubuntu
          ansible_ssh_private_key_file: ~/.ssh/network-sim.pem
          node_class: cloud
          wireguard:
            interface: wg0
            listen_port: 51820
            private_key: "{{ lookup('env', 'WG_EC2_NODE_1') }}"
            address: 10.42.0.10/32
            peers:
              - name: onprem-1
                public_key: <peer-public-key>
                endpoint: onprem.example.com:51820
                allowed_ips: ["10.42.0.20/32"]
    onprem:
      hosts:
        lab-router:
          ansible_host: 192.0.2.33
          ansible_user: labadmin
          node_class: onprem
          wireguard:
            interface: wg0
            listen_port: 51820
            private_key: <local-private-key>
            address: 10.42.0.20/32
            peers:
              - name: ec2-node-1
                public_key: <peer-public-key>
                endpoint: 3.80.1.23:51820
                allowed_ips: ["10.42.0.10/32"]
```

The CLI is free to embed the actual keys or reference external lookups (for
example via Ansible Vault).

## Playbook usage

Run the full configuration (dependencies + WireGuard) once the inventory is
generated:

The setup command already invokes Ansible after Terraform succeeds, but you can
rerun it manually for debugging:

```bash
(cd exp_management/ansible && ansible-playbook playbooks/site.yml)
```

The playbook is safe to rerun; idempotent tasks only change the machines when
package versions or WireGuard peer definitions differ.

## Role overview

- `roles/common`: installs baseline packages (curl, git, python3, etc.), Docker,
  and any node-class specific dependencies (AWS CLI for cloud nodes, hardware
  monitoring for on-prem, etc.).
- `roles/wireguard`: installs WireGuard, writes `/etc/wireguard/<interface>.conf`
  from the host vars, and ensures the service is enabled and restarted when
  peer topology changes.

When on-prem hosts require special tooling (for example, BMC utilities) add the
packages to `common_onprem_packages`. Likewise cloud hosts can receive AWS
specific agents through `common_cloud_packages`. The CLI only needs to make
sure `node_class` is set so these conditionals fire.
