# 0-ai-trust pipeline dependency map

The Databricks graph is generated dynamically from the dataset registries and
the Silver model registration functions. The diagram below is intentionally
grouped by source type and subject area so that it remains readable. The
actual pipeline still creates one Bronze table and one Silver entity for every
dataset listed in the registries.

```mermaid
flowchart LR
    classDef source fill:#eef2ff,stroke:#4f46e5,color:#111827
    classDef bronze fill:#fff7ed,stroke:#ea580c,color:#111827
    classDef silver fill:#ecfdf5,stroke:#059669,color:#111827
    classDef control fill:#fef2f2,stroke:#dc2626,color:#111827
    classDef gold fill:#f5f3ff,stroke:#7c3aed,color:#111827

    subgraph S[Source landing]
        DB[(Database CDC landing)]:::source
        EV[(Event landing)]:::source
        FI[(File landing)]:::source
        MF[(Manifest landing)]:::source
    end

    subgraph B[Bronze streaming tables]
        BC[cdc_* source-aligned tables]:::bronze
        BE[event_* source-aligned tables]:::bronze
        BF[file_* source-aligned tables]:::bronze
        CM[control_ingestion_manifests]:::control
        IQ[ingestion_quarantine]:::control
    end

    DB --> BC
    EV --> BE
    FI --> BF
    MF --> CM
    BC -. rescued transport rows .-> IQ
    BE -. rescued transport rows .-> IQ
    BF -. rescued transport rows .-> IQ

    subgraph V[Contract validation and staging]
        SV[private validated source views\n_source_*_validated]:::control
        Q[quarantine.record_failures]:::control
        DQ[quarantine.dependency_violations]:::control
    end

    BC --> SV
    BE --> SV
    BF --> SV
    SV -. hard-rule failures .-> Q

    subgraph P[Silver subject areas]
        IP[involved party\nip_individual, ip_organisation, ip_kyc,\nip_party_relationship, ip_org_relationship]:::silver
        AP[application\napp_application, app_stage_history,\napp_status_change, app_missing_document,\napp_lifecycle_event, app_accepted_loan,\napp_rejected_application]:::silver
        AR[arrangement\narr_banking_arrangement, arr_loan,\narr_mortgage, arr_credit_card]:::silver
        SE[event and service\nevt_service_case, evt_support_interaction,\nevt_case_event]:::silver
    end

    SV --> IP
    SV --> AP
    SV --> AR
    SV --> SE
    IP --> DQ
    AP --> DQ
    AR --> DQ
    SE --> DQ

    subgraph G[Gold SQL marts]
        GD[dimensions]:::gold
        GA[application_mart]:::gold
        GR[arrangement_mart]:::gold
        GP[party_verification_mart]:::gold
        GS[service_mart]:::gold
        GC[context_mart]:::gold
    end

    IP --> GD
    AP --> GA
    AR --> GR
    IP --> GP
    SE --> GS
    IP --> GC
    AP --> GC
    AR --> GC
    SE --> GC
```

## Why the native Databricks graph looks complicated

The graph contains more nodes than the business model because one Python
registration call expands into several pipeline objects. For each Silver
entity, the framework can create a private validation view, expectation rules,
a quarantine flow, and either an AUTO CDC SCD2 target or an append-only
streaming target. These are implementation nodes, not additional business
tables.

There are also four different dependency patterns. Direct CDC entities use
`create_auto_cdc_flow`, immutable entities use a streaming table with a
watermark and deduplication, joined entities use a private materialized view
followed by `create_auto_cdc_from_snapshot_flow`, and cross-entity checks read
several canonical Silver tables to produce dependency violations. The UI draws
all of these edges, including validation and quarantine branches, which makes
the screenshot appear tangled.

The source-to-Bronze side is intentionally fan-out. The registry loops create
one source-aligned table for every database, event, and file dataset so that
each dataset keeps its own schema, checkpoint, CDF history, and lineage. This
is safer for schema evolution and replay than one universal Bronze table, but
it produces many parallel nodes in the UI.

The grouped diagram is therefore the useful communication view. The Databricks
lineage view remains the operational view for tracing one concrete dataset,
rule, or failure record.
