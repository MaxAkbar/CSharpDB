# CSharpDB.DataPrivacy

Reusable manual privacy/retention policies for standard direct-file and durable hybrid CSharpDB databases. The library contains versioned policies, eligibility rules, relationship mappings, masking operations, bounded previews, atomic execution, persistence, and run reconciliation. It has no Razor dependency.

See the [Privacy & Retention guide](../../docs/privacy-retention.md) for the Studio workflow, API example, date/masking semantics, execution limits, preservation rules, and recovery procedure.

The [self-contained HTML guide](../../docs/privacy-retention.html) supports offline reading and printing. The website edition is [www/docs/privacy-retention.html](../../www/docs/privacy-retention.html). Regenerate both from the Markdown guide with `scripts/Export-PrivacyRetentionGuide.ps1`.

The core entry points are `PrivacyPolicyStore.SaveAsync`, `PrivacyService.ValidateAsync`, `PreviewAsync`, `ApplyAsync`, and `ReconcileAsync`. A prepared preview is transient, disposable, and single-use. Apply requires the original client and unchanged policy/database inputs. Connections must support transactional snapshots and exclusive sessions; a private file-writer barrier prevents competing engine handles from bypassing the transaction.

Run the integration and component tests with:

```powershell
dotnet run --project tests/CSharpDB.DataPrivacy.Tests/CSharpDB.DataPrivacy.Tests.csproj
```
