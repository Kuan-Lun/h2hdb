# Core／Ingest 精簡任務紀錄

這是跨對話接續用的任務範圍、候選與驗收狀態，不是第二份代理政策。
政策以各 repository 的 `AGENTS.md` 為準；Core 入口為 [AGENTS.md](../AGENTS.md)。
本階段先檢查功能必要性、責任、狀態與重複工作，再決定是否拆包；LOC 減幅不是目標。
本次只建立接續機制，所有 runtime 精簡與拆包候選均未完成。

## 已核對基線

下表是 2026-10-09 各 checkout 的已提交來源，不代表同時部署、已發布或相容組合。
LOC 是 tracked runtime `.py` 的實體行數，包含註解、空白與 docstring。

| Repository | Commit | Project version | 模組／LOC |
| --- | --- | --- | --- |
| Core／`h2hdb` | `7ebbb03bffe42b51df4ab9d6228d639fbe5b75ac` | `0.45.6` | 94／117,084，含 generated wrapper 24 行 |
| Ingest／`h2hdb-ingest` | `c5a69677c4cb40d87cace453a8159e31a3ed109e` | `0.30.3` | 48／20,091 |

Core 歷史比較另列 generated Python，避免將 schema 展開量誤認為手寫程式成長：

| 歷史點 | 精確 commit | 非 generated LOC | Generated Python LOC |
| --- | --- | --- | --- |
| `NEW!!!` 前 | `f1fe0f911de20b718a79da4c53d88f593dbaab4d` | 9,199 | 0 |
| `NEW!!!` | `662243f5cfd83a90f7148f85f381c6ddbde45266` | 9,246 | 0 |
| Greenfield ingest | `d43c4fcb13fd05ffb3b15ef8c7be38aef96a0ba7` | 78,352 | 504,775 |
| 本次 Core 基線 | `7ebbb03bffe42b51df4ab9d6228d639fbe5b75ac` | 117,060 | 24 |

以上可由各 ref 的 `git ls-tree`、`git show` 與 `pyproject.toml` 重建；不是效能測量。
`82e3f196272565e827d880df1113ab01b36234ca` 已刪除舊 service、migrations、
canonical/catalog repositories 與 table repositories；不能由 `vnext` 名稱推定雙軌仍存在。

## 已知與未證實

- 已知：上述歷史差異、模組盤點及 R02 的相同函式 body 可由 Git source 核對。
- 已知：codecs 有純函式邊界；schema、cleanup、fencing 有跨表與交易耦合。
- 未證實：任何候選可安全移除、整體狀態數可減少、拆包有淨收益或達成成本目標。
- 先前聊天與暫存 logs 只提供線索；量化結論須重新取得有來源版本的持久報告。
- 尚未執行本計畫的 runtime 實驗、正確性測試、效能驗收或完成範圍複查。

## 範圍清單

基線來自兩 repo 的 `git ls-files 'src/**/*.py'`。下列模式匹配模組檔名、不含 `.py`；
Core 根為 `src/h2hdb/`，Ingest 根為 `src/h2hdb_ingest/`。
這些分組覆蓋 94／48 個 tracked 模組，基線中無重複或遺漏；全部僅「已盤點、待審核」。
新增模組或 HEAD 變動時，差異仍待分類；候選表不取代完整範圍清單。

| ID | 功能範圍 | 數量 | 模組模式 |
| --- | --- | --- | --- |
| C01 | Source、observation、staging | 16 | `vnext_source_*`, `vnext_gallery_staging_*`, `vnext_gallery_identity_repository`, `vnext_manifest_family` |
| C02 | Analysis、decision、hash reuse | 8 | `vnext_analysis_*`, `vnext_file_decision_validation_plan`, `vnext_hash_cache_repository` |
| C03 | Publication、artifact、activation | 10 | `vnext_publication_*`, `vnext_artifact_*`, `vnext_library_activation_repository` |
| C04 | 公開 orchestration／facades | 4 | `vnext_ingest_facade`, `vnext_ingest_analysis`, `vnext_ingest_publication`, `vnext_facade` |
| C05 | Cleanup、canonical persistence | 5 | `vnext_cleanup_*`, `vnext_canonical_value_*`, `vnext_canonical_consumer` |
| C06 | Coordination、queue、transaction | 10 | `vnext_allocator_repository`, `vnext_download_ingest_repository`, `vnext_ingest_fence_repository`, `vnext_ingest_policy_repository`, `vnext_maintenance_gate_repository`, `vnext_operational_event_repository`, `vnext_queue_repository`, `vnext_storage_instance_repository`, `vnext_transaction`, `vnext_state_machine_contract` |
| C07 | Schema、validators、audit、capacity | 12 | `schema_*`, `vnext_schema_provider`, `_generated_vnext_schema`, `_schema_artifact_codec`, `catalog_refinement`, `operational_refinement`, `source_collection_refinement`, `catalog_writer`, `database_audit`, `vnext_physical_domains`, `vnext_capacity` |
| C08 | Codecs、domain、ports、errors | 9 | `vnext_identity`, `vnext_domains`, `catalog_search`, `domain`, `ports`, `artifact_errors`, `artifact_resources`, `catalog_errors`, `source_errors` |
| C09 | Catalog read／identity／registry | 3 | `vnext_catalog_*` |
| C10 | DB transport、pool、composition、clock | 6 | `*connector`, `mariadb_pool`, `repository`, `database_clock` |
| C11 | Telemetry、logging | 6 | `*performance`, `ingest_performance_format`, `logger` |
| C12 | 啟動、設定、exports | 5 | `__init__`, `__main__`, `config_loader`, `environment`, `settings` |
| I01 | Source observation／scheduling | 5 | `core_source`, `filesystem`, `source_monitor`, `source_schedule`, `_source_retry` |
| I02 | Media／qualification／artifact policy | 6 | `artifact`, `artifact_errors`, `image_qualification`, `page_workers`, `policy`, `source_image` |
| I03 | Library、storage、journal、cleanup | 13 | `library`, `library_identity`, `library_relocation`, `_library_journal`, `_library_layout`, `_library_maintenance`, `_relocation_files`, `_resource_cleanup`, `_storage_paths`, `storage`, `storage_capacity`, `scratch`, `relocate` |
| I04 | Resident、session、runtime lifecycle | 7 | `bootstrap`, `database_audit`, `maintenance`, `resident`, `runtime`, `service`, `session` |
| I05 | Telemetry、diagnostics、progress | 13 | `*performance`, `image_diagnostics`, `metrics`, `progress`, `progress_format`, `_diagnostic_logging`, `_log_fields`, `_log_recovery`, `_retry_diagnostics` |
| I06 | 啟動、設定、exports、limits | 4 | `__init__`, `__main__`, `config`, `_limits` |

Tests、schema manifests、formal models、scripts 與 deployment 是驗證／影響範圍，
本階段不以精簡這些檔案為目標。Core／Ingest 以外 consumers 僅按受影響介面納入驗收。

## 候選與處置

狀態區分待審核、調查中、待實驗、待決策、採用待驗收、完成、拒絕、延期。
拒絕與延期不是實作完成；保留理由及新證據下的重開條件。以下尚無完成項目。

| ID | 候選／狀態 | 持久證據入口 | 結案或重開條件 |
| --- | --- | --- | --- |
| R01 | Issue／prepare／commit 責任與狀態；待審核、下一輪 | C01–C04；`vnext_ingest_analysis.py` 的 `issue_analysis_step`、`prepare_analysis_step`、`commit_analysis_step` | 一個步驟完成 authority／狀態／失敗路徑對照後，判定保留、合併或實驗；契約或路徑改變時重開 |
| R02 | 相同 session／page predicates；待審核 | `vnext_ingest_facade.py`、`vnext_ingest_analysis.py`、`vnext_ingest_publication.py` 的 session identity；facade／source spool 的 named/tag page validators | 證明可共用規則且各信任邊界仍驗證；若語意不同則拒絕，日後語意收斂再開 |
| R03 | READY audit／cleanup 重複工作；待重新量測 | C05、C07；`catalog_refinement.py`、`vnext_cleanup_repository.py`；既有成本工具 | 先固定工作量契約與反例再量測；缺可靠 provenance 時不判達標 |
| R04 | 純 codecs distribution；延期至邊界審核 | C08；`vnext_identity.py`、`catalog_search.py`、`catalog_writer.py`、`database_audit.py` | 先確認 byte／Unicode／guard ownership／audit 失效契約；邊界與收益成立後再決定拆包 |
| R05 | Media distribution；待審核 | I02 與 I01／I03 使用點；Ingest `AGENTS.md` 的 ownership boundary | 先證明與 source authority、storage、journal 的可分離性及驗收範圍 |
| R06 | Transport／telemetry／contracts；延期 | C08、C10、C11、I05；實際 import 與 consumer 使用點 | 有獨立使用需求、可減少依賴或發布耦合的證據再開；檔案大小不是理由 |
| R07 | Library／relocation 重複格式規則；待審核 | I03；`library.py` 的 `_marker_payload`、`_quarantine_leaf` 與 `library_relocation.py` 的 `_publication_marker`、`_quarantine` | 核對同一 marker／hash framing 的共用邊界與錯誤語意；規則不同則保留並記錄差異，格式變更時重開 |

## 下一輪入口

接續時核對兩 repo 的 HEAD、工作樹及政策；實際 checkout 由當次提供，不固定 sibling 路徑。
本輪建議只處理 R01 的「file decision validation 一個 bounded step」，先不拆 package。
入口為 [analysis orchestration](../src/h2hdb/vnext_ingest_analysis.py)、
[analysis repository](../src/h2hdb/vnext_analysis_repository.py) 與
[validation plan](../src/h2hdb/vnext_file_decision_validation_plan.py)。
產出同一資料從來源、plan、checkpoint 到 commit 的責任／狀態對照，
區分「相同規則的重複實作」「不同信任邊界的必要驗證」「可重用的重複工作」。
若有可優化假設，下一步才是具固定預算與反例的隔離實驗；收益和正確性成立後再實作。
這不代表 C01–C04 已審核完成，也不預設一定有程式可刪。

## 每輪交付與證據

- 一個內聚候選的問題、必要責任、行為變化、假設、反例、採用或不採用理由。
- 涉及修改時，記錄實作 commit、影響範圍及實際執行的既有檢查；未跑、skip 分開列明。
- 效能主張附事前成本契約、中大型資料形狀、固定／逐筆成本及 baseline／candidate provenance。
- Ledger 只保存摘要和證據索引：repo／ref、相對路徑或 artifact ID、hash、命令與結果。
  大型 logs 不複製進本文；缺失證據記為待重新取得，正式 receipts 不由本文取代。
- 本輪完成前回寫候選、範圍審核狀態與下一個入口；只改 ledger 不代表 runtime 通過。
  實作 commit 可先記錄，最後 merge 由 Git history 核對，避免自我引用 commit hash。

調查輪完成指該單元有可核對的結論與下一步，不代表其所屬範圍已全部審核。
實作輪完成另需相關 repository 整合與必要驗收的真實證據；待整合、待實驗、
待決策或缺少證據的候選仍未完成。沒有收益的候選可以具體理由結案，不強行修改。

可重用 [驗證說明](../verification/README.md)、[語意證據索引](../verification/invariants.toml)、
[Core SQL 成本工具](../scripts/check-ingest-database-performance.py)，以及 Ingest 的
`scripts/check-source-cost.py`、`scripts/check-library-cleanup-cost.py`。
既有規則適用的 gate、人工驗收與 review 仍由各 repo scripts 負責；本文不新增 gate。

## 本階段完成條件

本階段指上述固定 runtime 範圍的必要性／複雜度審核，不代表全庫已達理論最簡。

1. 每個 C／I 分組及接續期間新增模組都有審核記錄，無未分類缺口。
2. 每個發現都有可追溯處置；延期須列原因、重新開啟條件及尚未達成的目標。
   未完成的範圍內必要工作不能用延期標記充當完成；範圍變更另記明確決策。
3. 採用項目已整合並完成其必要驗證；程式正確、成本達標、部署可用分別記錄。
4. 在最終來源版本重新排查功能責任、重複工作與耦合，沒有遺漏的可處理範圍內問題。
5. 拆包各自有採用／拒絕／後續階段決定與依據；拆包數量和 LOC 減少都不是完成指標。

目前進度：機制初始化；完整範圍僅盤點，R01 尚未開始，以上完成條件尚未滿足。
