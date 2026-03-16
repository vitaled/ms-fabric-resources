# Azure Analysis Services Migration Toolkit

Tools and scripts for migrating **Azure Analysis Services (AAS)** workloads to **Microsoft Fabric**.

## Repository Structure

| Folder | Description |
|---|---|
| [aas-to-fabric/](aas-to-fabric/) | Migrate an AAS tabular model to a Fabric semantic model (direct XMLA) or via a Lakehouse pipeline (Lakehouse → Dataflow Gen 2 → Semantic Model). |
| [aas-firewall-update/](aas-firewall-update/) | Automatically update AAS firewall rules with Azure Service Tag IP ranges (e.g. for Dataflow Gen 2 connectivity). |
| [scripts/](scripts/) | Utility scripts — currently includes automated On-Premises Data Gateway installation and registration. |

## Quick Links

- **Migrate a model** — [aas-to-fabric/README.md](aas-to-fabric/README.md)
- **Update AAS firewall** — [aas-firewall-update/README.md](aas-firewall-update/README.md)
- **Install a Data Gateway** — [scripts/README.md](scripts/README.md)

## Prerequisites

| Tool | Used by |
|---|---|
| Python 3.10+ | `aas-to-fabric/` |
| .NET 6+ runtime | `aas-to-fabric/` (pythonnet) |
| PowerShell 5.1+ | `aas-firewall-update/`, `scripts/` |
| Az PowerShell modules | `aas-firewall-update/` |
| DataGateway module | `scripts/` |

## Getting Started

Each subfolder is self-contained with its own README. Pick the tool you need and follow the instructions in the corresponding folder.
