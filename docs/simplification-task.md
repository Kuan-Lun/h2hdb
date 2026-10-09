# H2HDB 全系統精簡任務紀錄

這是跨對話接續用的任務範圍、候選與驗收狀態，不是第二份代理政策。
政策以各 repository 的 `AGENTS.md` 為準；Core 入口為 [AGENTS.md](../AGENTS.md)。
目標是六個自有程式 repository 與部署 workspace 的整體精簡：先檢查功能必要性、
責任、狀態與重複工作，再決定是否拆包；LOC 減幅不是目標。
2026-10-09 使用者將原 Core／Ingest 計畫擴至整套系統，其他 workspace 也需主動審核。
範圍擴充已整合；目前完成 R01 一個 bounded step 的來源調查，
尚未採用 runtime 精簡或拆包。

## 已核對基線

下表是 2026-10-09 各 checkout 的已提交來源，不代表同時部署、已發布或相容組合。
LOC 是 tracked runtime `.py` 的實體行數，包含註解、空白與 docstring。

| Repository | Commit | Project version | 模組／LOC |
| --- | --- | --- | --- |
| Core／`h2hdb` | `b8e3a06ab3bb91ad7fe07518383327af95d5ce82` | `0.45.6` | 94／117,084，含 generated wrapper 24 行 |
| Ingest／`h2hdb-ingest` | `c5a69677c4cb40d87cace453a8159e31a3ed109e` | `0.30.3` | 48／20,091 |
| OPDS／`h2hdb-opds` | `03be557ebd78763b74288f7c39a9e4934934148b` | `0.24.2` | 23／6,384 |
| Komga sync／`h2hdb-komga` | `0182984a1357f5a1345d3dd78ff8dca94f0cf597` | `0.18.2` | 7／1,511 |
| Downloader／`h2hdb-downloader` | `e6fbe969a3300dd90bb4174fa3e08e7e7b4e3053` | `0.23.2` | 3／1,367 |
| Browser／`hbrowser` | `b8bf1ba306f3222ca12cb589d4d10e08e7a25fc3` | `0.44.2` | 49／18,827 |

六個套件共 224 個 Python 模組、165,240 行非 generated runtime。
部署 workspace 沒有 `.git` 或 `pyproject.toml`；HEAD、Git clean status 與
project version 均不適用。其基線為下方 P01–P07 的 23 個自有控制檔案，
共 5,598 實體行，不與上述 runtime LOC 混算；來源摘要 SHA-256：
`760f80dd57cac46e1d44e773689b8b2cf39d54ac9f10e0299aae730df04a790c`。
摘要算法：相對路徑排序後串接 `path + NUL + file_sha256_hex + newline`，再取 SHA-256。
這不證明執行中的映像或套件版本；那些版本尚未查證。

Core 歷史比較另列 generated Python，避免將 schema 展開量誤認為手寫程式成長：

| 歷史點 | 精確 commit | 非 generated LOC | Generated Python LOC |
| --- | --- | --- | --- |
| `NEW!!!` 前 | `f1fe0f911de20b718a79da4c53d88f593dbaab4d` | 9,199 | 0 |
| `NEW!!!` | `662243f5cfd83a90f7148f85f381c6ddbde45266` | 9,246 | 0 |
| Greenfield ingest | `d43c4fcb13fd05ffb3b15ef8c7be38aef96a0ba7` | 78,352 | 504,775 |
| 初始化 runtime 基線 | `7ebbb03bffe42b51df4ab9d6228d639fbe5b75ac` | 117,060 | 24 |

以上可由各 ref 的 `git ls-tree`、`git show` 與 `pyproject.toml` 重建；不是效能測量。
`82e3f196272565e827d880df1113ab01b36234ca` 已刪除舊 service、migrations、
canonical/catalog repositories 與 table repositories；不能由 `vnext` 名稱推定雙軌仍存在。

R01 接續核對：六個主要 checkout 都乾淨且各有獨立 Git common directory；
除 Core 外，HEAD 與上表前次基線一致。Core 從 `13e9277` 至 `b8e3a06`
只改本紀錄，前次範圍擴充 commit `9ba36e8` 已是 HEAD ancestor，
`release-gate.py status HEAD` 確認其 documentation receipt 有效。
Core 尚有未合併的 `formal/close-production-blockers`（`5b8fe391`，無 worktree）；
Ingest 另有乾淨的 `feat/page-worker-decision-log` worktree（`d7c8ef7b`，未合併）。
兩者均保留，不推定其工作已失效或接管；本輪只讀上表來源、只修改 Core 本紀錄，
未修改它們的程式範圍。部署 P01–P07 的 23 檔 hash 重算與既有摘要相同。

## 已知與未證實

- 已知：上述歷史差異、模組盤點及 R02 的相同函式 body 可由 Git source 核對。
- 已知：codecs 有純函式邊界；schema、cleanup、fencing 有跨表與交易耦合。
- 已知：R01 file validation 的 local plan 已跨頁重用；issue／prepare／commit
  分別擁有 durable coordinates、local preparation 與 fresh commit authority。
- 未證實：任何候選可安全移除、整體狀態數可減少、拆包有淨收益或達成成本目標。
- 先前聊天與暫存 logs 只提供線索；量化結論須重新取得有來源版本的持久報告。
- 尚未執行本計畫的 runtime 實驗、正確性測試、效能驗收或完成範圍複查。

## 系統邊界與階段

以當次 workspace 輸入與各套件 project name 定位，不要求固定 sibling 路徑。
部署入口以實際 Compose、Dockerfile 及 resolver 定位；表中角色是本次快照，
後續角色與依賴仍從來源重讀，不把這份表當成另一份部署 manifest。

| 邊界 | 目前來源與契約 | 受影響時的範圍 |
| --- | --- | --- |
| Core → Ingest／OPDS／Komga sync／Downloader | 四個 `pyproject.toml` 皆要求 `h2hdb>=0.43.0,<0.46.0`；公開 facade、immutable values、READY 與 log | 實際呼叫者、版本解析、對應套件與部署組合 |
| hbrowser → Downloader | Downloader 要求 `hbrowser>=0.44.0,<0.45.0`；browser lifecycle、錯誤、下載提交與取消 | D02–D03、H01–H06、部署下載入口 |
| Ingest → OPDS／Komga sync／Komga server | 已發布檔案、CBZ／page extents、storage path、`publication.lock` 與 `ACTIVATING` | I02–I04、O04、K02–K03、P01／P03；不只看 Python imports |
| 共用 image build → 全部套件角色 | 部署 `Dockerfile.runtime` 呼叫 `runtime/cohort.py`，解析 Core 與四個 role | 常駐 Ingest／OPDS 與 `jobs` profile 的 Downloader／Komga sync 都納入 |

部署 resolver 先各自選版本，再驗證 exact Core／role 組合；不相容即失敗。
`hbrowser>=0.44.1` 是 Downloader 的額外解析條件，不是 cohort 獨立固定版號的第六個套件。
Komga Java server、H@H、galleryinfo parser 及其他第三方套件是外部整合邊界；
本計畫不將它們的上游程式碼或 production 資料當作自有精簡目標。

| 階段 | 工作範圍 | 目前狀態 |
| --- | --- | --- |
| S0 | 七個 workspace 的來源與依賴盤點 | 本次建立基線；功能必要性仍未審核 |
| S1 | Core／Ingest 的責任、狀態與重複工作 | R01 file validation step 已調查；其餘範圍與候選驗證未完成 |
| S2 | OPDS、Komga sync、Downloader、hbrowser 的自身設計 | 待審核；不以 S1 是否碰到它們作為啟動條件 |
| S3 | 部署控制流程與跨 repository 工程工具重複責任 | 待審核；按實際契約和維護成本選單元 |
| S4 | 全系統重查與逐項拆包決定 | 待 S1–S3 的證據；不是自動發布套件 |

S1 是優先順序，不是其他範圍的豁免。相依介面有變更時，相關驗收提前同輪處理；
獨立候選可調整順序並記理由。完成單輪或 S1 都不代表整個計畫完成。

## 範圍清單

套件基線來自各 `pyproject.toml` 的 wheel package roots 之下的 tracked `.py`。
下列模式匹配相對 package root 的路徑、不含 `.py`。
Core／Ingest／OPDS／Komga／Downloader 根各為 `src/h2hdb*` 的對應 package；
hbrowser 根為 `hbrowser/`，保留 `gallery/` 等子路徑。
分組覆蓋 94／48／23／7／3／49 個模組，基線中無重複或遺漏。
C02／C04 與 I04 已局部追蹤 R01 呼叫路徑，所有分組的完整審核仍未完成。
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
| O01 | 啟動、設定、authentication、routing | 6 | `__init__`, `__main__`, `app`, `config`, `auth`, `routing` |
| O02 | Catalog、discovery、search、cursor／recovery | 6 | `catalog_service`, `browse`, `discovery`, `search`, `cursor`, `recovery` |
| O03 | OPDS 協定、serialization、publication | 5 | `atom`, `opds12`, `opds2`, `serialization`, `publication` |
| O04 | Acquisition、media、library fencing | 3 | `acquisition`, `media`, `library` |
| O05 | Language、URI／URL | 3 | `language`, `uri`, `urls` |
| K01 | 啟動、設定、exports | 3 | `__init__`, `__main__`, `config_loader` |
| K02 | Sync、settling、coordination | 2 | `sync`, `coordination` |
| K03 | Komga REST 與 metadata mapping | 2 | `komga`, `metadata` |
| D01 | 公開 exports | 1 | `__init__` |
| D02 | Queue、CSV replay、Core facade adapter | 1 | `_queue` |
| D03 | Download／cascade、turn／heartbeat／handoff | 1 | `downloader` |
| H01 | Driver、navigation、login、element actions | 5 | `gallery/driver_base`, `gallery/eh_driver`, `gallery/exh_driver`, `gallery/element_action`, `gallery/forums_auth` |
| H02 | Browser、owned processes、transport helpers | 16 | `gallery/browser/*` |
| H03 | Challenge detection／policy | 7 | `gallery/captcha/*`, `gallery/challenge_policy` |
| H04 | Gallery／search／punch-in values | 3 | `gallery/models`, `gallery/punchin_models`, `gallery/search_models` |
| H05 | Logging、diagnostics、deadline、page utilities | 13 | `gallery/utils/*` |
| H06 | Exports、errors、notification | 5 | `__init__`, `gallery/__init__`, `exceptions`, `beep`, `notify` |

部署控制檔與工程工具另列如下；它們也有必要性與重複責任的審核工作。
Tests、schema manifests、formal models 與政策是需求／驗收的證據，
不以減少這些證據的數量當作成果。受影響的測試或模型隨實作調整。

| ID | 部署控制範圍 | 檔數 | 相對部署根目錄的完整檔案清單 |
| --- | --- | --- | --- |
| P01 | Compose／build | 4 | `docker-compose.yml`, `Dockerfile.runtime`, `Dockerfile.komga`, `.dockerignore` |
| P02 | 套件解析 | 1 | `runtime/cohort.py` |
| P03 | Runtime entrypoints／啟動 fencing | 5 | `runtime/main.sh`, `runtime/h2hdb-downloader.py`, `runtime/komga-start.sh`, `runtime/publication-ready.sh`, `runtime/wait-opds.py` |
| P04 | 隔離整合／canary | 4 | `scripts/check-compose-cohort.py`, `scripts/check-compose-startup.py`, `scripts/canary-h2hdb-ingest-image.sh`, `scripts/canary-h2hdb-ingest-inspect.py` |
| P05 | 部署操作／掛載驗證 | 2 | `scripts/deploy-h2hdb-cohort.sh`, `scripts/compose-bind-sources.py` |
| P06 | 回歸／build 平台工具 | 5 | `scripts/test-cohort.py`, `scripts/test-compose-bind-sources.py`, `scripts/test-downloader.py`, `scripts/test-startup.py`, `scripts/check-dockerfile-platforms.py` |
| P07 | 政策與忽略設定 | 2 | `AGENTS.md`, `.gitignore` |

P01–P07 全部已盤點、待審核；P07 的政策只由原 `AGENTS.md` 定義。
清單只包含自有控制來源，排除 `.env`、`env/`、`config/`、`state/`、library、
credentials、日誌與使用者資料。套件／服務設定介面仍由程式與部署控制來源審核，
不靠讀取實際 secrets。後續自有控制檔新增、移除或 hash 變動需重新核對範圍。

| ID | 六個程式 repository 的工程工具審核面 | 狀態／證據入口 |
| --- | --- | --- |
| E01 | 共用開發／Git／release 工具的重複責任 | 待逐檔細分；各 repo 的 `scripts/`、`.githooks/`、`.github/workflows/` 與對應設定 |
| E02 | 驗收工具與跨庫 fixture 的責任邊界 | 待逐檔細分；各 repo 的 performance／integration／backend coverage 工具及其測試 |

E01–E02 是跨庫審核面，並非上方 runtime 模組的互斥計數。
S3 開始時由各基線的 tracked 輔助程式和設定建立逐檔歸屬，交集只記一次；
尚未完成這項工具細分，不能宣稱整個 workspace 已完整審核。

## 候選與處置

狀態區分待審核、調查中、待實驗、待決策、採用待驗收、完成、拒絕、延期。
拒絕與延期不是實作完成；保留理由及新證據下的重開條件。
R01 已完成下述調查單元，候選整體及其他項目仍未完成。

| ID | 候選／狀態 | 持久證據入口 | 結案或重開條件 |
| --- | --- | --- | --- |
| R01 | File validation step 調查完成；保留三階段邊界，局部重複候選待驗證 | C02／C04、I04；下方 R01 調查結果及來源／測試索引 | 同交易 baseline/current 重讀先固定成本契約與反例再實驗；其他 analysis stages 仍待審核 |
| R02 | 相同 session／page predicates；待審核 | `vnext_ingest_facade.py`、`vnext_ingest_analysis.py`、`vnext_ingest_publication.py` 的 session identity；facade／source spool 的 named/tag page validators | 證明可共用規則且各信任邊界仍驗證；若語意不同則拒絕，日後語意收斂再開 |
| R03 | READY audit／cleanup 重複工作；待重新量測 | C05、C07；`catalog_refinement.py`、`vnext_cleanup_repository.py`；既有成本工具 | 先固定工作量契約與反例再量測；缺可靠 provenance 時不判達標 |
| R04 | 純 codecs distribution；延期至邊界審核 | C08；`vnext_identity.py`、`catalog_search.py`、`catalog_writer.py`、`database_audit.py` | 先確認 byte／Unicode／guard ownership／audit 失效契約；邊界與收益成立後再決定拆包 |
| R05 | Media distribution；待審核 | I02 與 I01／I03 使用點；Ingest `AGENTS.md` 的 ownership boundary | 先證明與 source authority、storage、journal 的可分離性及驗收範圍 |
| R06 | Transport／telemetry／contracts；延期 | C08、C10、C11、I05；實際 import 與 consumer 使用點 | 有獨立使用需求、可減少依賴或發布耦合的證據再開；檔案大小不是理由 |
| R07 | Library／relocation 重複格式規則；待審核 | I03；`library.py` 的 `_marker_payload`、`_quarantine_leaf` 與 `library_relocation.py` 的 `_publication_marker`、`_quarantine` | 核對同一 marker／hash framing 的共用邊界與錯誤語意；規則不同則保留並記錄差異，格式變更時重開 |
| R08 | OPDS／Komga reader fencing 共用規則；待審核 | O04、K02；OPDS `library.py` 與 Komga `coordination.py` | 比較開檔機制、錯誤分類與持鎖期限；request 與整次 sync 的生命週期不能直接等同 |
| R09 | Downloader root／batch 工作生命週期；待審核 | D02–D03；`downloader.py` 的 `_run_coordinated_batch`、`_run_coordinated_root` | 對照 heartbeat／handoff 與不同 completion 邊界後，判定共用規則或保留；契約變更時重開 |
| R10 | Browser ownership／deadline／diagnostics；待審核 | H01–H06；`gallery/browser/`、`gallery/utils/` 及 driver 呼叫者 | 先驗證狀態與資源所有權；兩次 fresh empty 的 confirmed-missing 證據不能視為無用重讀 |
| R11 | OPDS 協定與 catalog mapping；待審核 | O01–O05；`opds12.py`、`opds2.py`、`atom.py`、`serialization.py` | 分清協定差異與重複映射責任，再決定可共用部分；協定差異本身不是冗碼 |
| R12 | 部署解析／啟動／驗收／操作責任；待審核 | P01–P07，包含全部 profile jobs 與共用 build stage | 驗證 resolver、啟動 probes、隔離驗收與 deploy 各自的責任；不得以讀過來源宣稱可部署 |
| R13 | 跨庫工程工具收斂；待逐檔盤點 | E01–E02；既有 scripts、hooks、workflow 與驗收 fixture | 先比較執行語意與 ownership，列出真正重複者；不能以抽共用套件取代各庫應有驗收 |

## R01：file decision validation step 調查

來源為上表 Core `b8e3a06`／Ingest `c5a6967`；本輪只更新紀錄，待 task branch
`docs/simplification-r01-validation` 整合。問題是三階段是否重複維護同一權威，
成功條件為完成呼叫者、durable／local state、失敗與重播對照，並對可疑重複作出
保留或待驗證判定；不以刪行數為成果，也不作效能達標主張。

下表函式均在 Core 的 `src/h2hdb/`；Ingest 呼叫者為
`src/h2hdb_ingest/service.py:synchronize_analysis`（405 起）及
`session.py:IngestSessionController.call/outside_session`（54／66）。
前者以 `session.call` 執行 issue／commit，以 `outside_session` 執行 prepare；
後者只在取得 facade 與確認失敗狀態時短暫持鎖，local work 不阻擋 heartbeat。

| 邊界 | 資料與狀態權威 | 失敗／重播責任 |
| --- | --- | --- |
| Issue：`vnext_ingest_analysis.py:issue_analysis_step`（378）；`vnext_analysis_repository.py:issue_next_batch`（1891） | 短 write transaction 從 durable run、stage、checkpoint 取得 generation、cursor、processed count、page limit 與 actual-key prefix；caller batch key 只作冪等識別，既有結果須讀 exact receipt；每頁上限 128，prefix 可多讀一筆 | 活躍 issue 保留於 handle，再次呼叫仍重驗 live session；既存 batch receipt 重發其 start coordinates，不能以已推進 checkpoint 代替 |
| 首次 prepare：repository `prepare_file_decision_validation_plan`（2238） | 先後兩次獨立 read transaction 驗 run／source manifest／seals；中間分頁讀所選 accepted observations 的 artists／occurrences，在 private SQLite 排序後產生具 MAC 的匿名定寬檔 | Plan 是可丟棄的 process-local cache，不是 DB authority；失敗關閉 scratch；合法 source writers 不得修改 sealed facts |
| 每頁 prepare：repository `prepare_file_decision_validation_page`（2277）；`vnext_file_decision_validation_plan.py:source_page`（161） | 重用同一 plan，以 checkpoint cursor 二分定位 expected keys，與 issued actual keys 合併，取最多 page limit；page 綁定 authority、batch 與 checkpoint，缺 expected 的 actual key 保留 `None` 供驗證其 absence／tombstone | 後續頁不重新建立 source plan、不開 Core DB；page MAC 與 record MAC 分別保護 prepared value 與 local file，不能取代 fresh SQL checks |
| Commit：repository `validate_file_hash_decision_batch`（1743）、`_require_file_validation_binding`（6975）與 `_require_file_decision_targets`（7094） | Fresh write transaction 重新驗 lease/generation、run/source/seals、checkpoint 與 actual prefix，exact-compare current／baseline families；checkpoint、batch receipt 與 terminal component seal 一起提交 | 新增 prefix orphan、stale page、seal 變更或 takeover 必須拒絕；terminal 是空頁且 live count 等於 source count，不只看 page digest |
| Response loss／restart：repository `_require_prepared_file_validation_replay`（7024）；orchestrator `_prepare_file_decision_validation_work`（766） | Commit 成功但回應遺失時保留 exact page／plan，依 durable receipt 的 start coordinates 重驗 family、cursor、count、seal；重播不直接相信成功旗標 | Preparation 失敗丟棄 plan；commit 失敗保留可重播資源；process restart 從 durable authority 重建 plan，成功 terminal 後先 retire、在 lock 外關閉 |

Bounded 指 issue／commit 的頁面與每次 source read 的界限；首次 prepare 仍遍歷整個
selected source 並排序，不是總計只處理 128 筆，也未證明固定 wall-clock 上限。
Actual prefix 可含被 tombstone 隱藏的 ancestor keys，不增加 validated live count；
key 數有界不代表所有 SQL planner scan／aggregate fanout 均有相同界限。

判定：保留 issue／prepare／commit 及三類 state。Durable checkpoint／receipt 是恢復權威，
local active issue／step 是 response-loss handle，plan 是跨頁可丟棄工作；合併它們不能
消除這些責任，反而可能讓 source scan／scratch I/O 進入 session lock 或 DB write。
本輪無 compatibility path 移除、公開行為或資料格式變更，也沒有引入新 abstraction。
Plan reuse 不承諾在每次 commit 偵測未改 immutable markers 的 unmanaged SQL source
竄改；獨立 READY audit 的責任仍保留，不能把本調查視為完整資料稽核證明。

重複分類與處置：

- **不同信任邊界、保留**：issue 與 commit 各讀 actual prefix；prepare 前後與 commit
  各驗 source/run binding；record/page MAC 與 durable family exact compare。
  中間可能發生 takeover、prefix 插入、seal 改變或 caller page 竄改，前次成功不可重用。
- **同一規則、待驗證**：`validate_file_hash_decision_batch`（1757）及其唯一 binding
  helper（6982）在同一 commit 呼叫同一 page 的 `verify()`。它只驗 in-memory fields、
  owner metadata 與 MAC，沒有再讀磁碟 plan；不是重複 source scan。
  尚未證明可安全移除其中一次，後續須核對入口、錯誤次序與 mutation 邊界。
- **同交易重讀、下一個調查單元**：`_require_file_decision_targets`（7105–7106）
  先載入 baseline resolved values，再載入 current evidence；兩者都經
  `_load_file_decision_evidence`（9732）的 layout／shadow／tombstone loaders。
  一般 overlay 的 current ancestry 包含完整 parent suffix（`_derive_layout`，5469–5473），
  有重疊 family reads。Policy 變更或 parent depth 16 的 compaction 則 self-only，
  但 baseline 仍保存（1226–1231），不能直接把 current ancestry 尾部當成 baseline。
  尚未量測成本，也未採用交易快取或共用 loader。

可核對的既有驗收來源（本輪只讀，**未執行**）：

- `tests/test_vnext_ingest_analysis_validation.py`：單次建 plan／跨頁 reuse（114）、
  後續 prepare 不開 DB（148）、重發 start coordinates（170）、fresh prefix orphan（220）、
  terminal／nonterminal response loss（279）、restart（327）、source seal 消失（398）、
  takeover（435）、prepare 期間續租（484）。
- `tests/test_vnext_file_decision_validation_plan.py`：計數 oracle、metadata／record／page
  竄改；`tests/test_vnext_analysis_decision_batch.py`：materialized family exactness。
  `verification/invariants.toml` 的相關 runtime／fault bindings 是證據索引，非本輪執行結果。
- Ingest `tests/test_service_vnext.py:test_analysis_orchestration_keeps_preparation_outside_bounded_calls`
  與 `tests/test_session_vnext.py` 是 consumer 邊界入口。
- `scripts/ingest_growth_hash_probe.py` 已有 source-read 與 validation backend-work
  計數入口；其工具或結構性斷言存在，不等於 baseline/current 重讀優化已達成本目標。
  目前 fixture 的 history 是未選取 source observations，不是 overlay lineage，
  depth-zero query expectations 不能直接涵蓋本候選。Whole-analysis 成本工具可作
  整體回歸，但本輪無綁定這個候選的中大型 baseline/candidate 報告。

已執行的核對：各 checkout `git status --short`、`git rev-parse HEAD`、
`git rev-parse --git-common-dir`、`git worktree list`、`git branch --no-merged`；
以 `pyproject.toml` 核對 name/version/range；23 個部署控制檔依上方算法重算 hash。
Core `git diff --stat 13e9277 HEAD` 僅有本紀錄，
`git merge-base --is-ancestor 9ba36e8 HEAD` 成功，
`.venv/bin/python scripts/release-gate.py status HEAD` 確認前次文件 receipt 有效；
`rg` 與函式來源讀取確認上述呼叫與證據入口。
`node_modules/.bin/markdownlint-cli2 docs/simplification-task.md` 的工作樹預檢回報
0 issues；不取代後續 exact candidate 文件 gate。
本輪 version impact 為 `none`（純文件），Core 保持 `0.45.6`，無 dependency manifest
變更，不重新產生 audit。此段寫入時尚待文件 commit／merge gate；正式結果由 Git history
及 exact candidate receipt 核對，不預寫通過。未執行 runtime、MariaDB、deep、成本、
跨套件 wheel resolution、Compose build／隔離流程或 production 驗收。

## R01：同交易 family 讀取實驗（2026-10-09）

實驗前基線為 Core `041f707`／Ingest `c5a6967`。候選只合併同一次呼叫內
baseline/current ancestry 的 family 讀取；兩個 fresh layout 檢查、各自解析順序及
既有 17-layer／128-key 上限保留，不跨交易或 issue／prepare／commit 快取。
公開 facade、schema、telemetry 與部署介面預期不變；若採用 runtime 修改，
候選 Core 為 `0.45.7`，四個 role 的 `>=0.43.0,<0.46.0` 皆容納它。
Ingest 是直接 analysis consumer，需驗證既有 orchestration/session 契約；OPDS、
Komga sync、Downloader 與 hbrowser 的介面未預定修改。部署共用 build 仍包含四個
role；本局部實驗不宣稱 index-backed Compose 或 production 已驗收。

事前固定的局部成本契約如下，不依候選測得平均調高預算：

- 每頁 K ≤ 128；每個 root ancestry ≤ 17，兩個 root 的去重聯集 U ≤ 34。
  非空 root 各保留 2 個 layout SQL；family SQL 為 `2 * ceil(U / 17)`，
  返回的 requested-grid rows 為 `2 * K * U`，singleton probes 至多 `6 * K * U`。
  同一次 pair 讀取不得重讀相同 ancestry/key coordinate；恢復原雙讀作退化反例。
- 小型涵蓋 1／127／128／129（跨頁）／257 keys；主要局部規模為 4,096／32,768
  retained decision keys，當輪完整驗證同等 key 數。另以固定 128 keys 比較兩種
  retained 規模，檢查無關資料成長不增加 point-read 工作。
- 涵蓋 genesis、baseline depth 1／8／16 的 overlay、policy-change self-only、
  baseline 17 layers 的 depth-16 compaction；34-layer 非 suffix 邊界只作有界
  正確性案例。Inherited-value reuse 分別 0%／95%，不得混稱跨 gallery hash 重用率。
  本 fixture 使用已聚合 artist scalar 分布，並不冒充完整 source artist 分布；
  故結果只支持 family 讀取熱點，public pipeline 正確性另由既有測試驗證。
- SQLite／MariaDB 分開量測，在同資料上交替 baseline/candidate，各至少三次完整循環。
  報告 SQL/native work、固定與逐 key local CPU、elapsed、Python peak memory。
  本機新增的回歸預算為每次 pair Python peak ≤ 8 MiB、CPU ≤ 1 ms/key、
  elapsed ≤ 5 ms/key；它們是本候選的固定驗收預算，不是既有全管線 SLO。
  中大型重疊形狀的 candidate median wall 必須低於 baseline，才能宣稱本機淨收益。
  非重疊案例記錄新增成本，不能以漸近改善或小型查詢減少代替主要規模實測。

本節建立時尚未量測或採用實作；結果與持久證據於同一任務內補入。

## 下一輪入口

接續時先核對七個 workspace 的可用性、來源基線與政策，再細讀當輪相關範圍。
缺失 workspace 保留為未完成項；只有當輪需要它才阻擋該單元，不據此縮小整體目標。
優先核對本輪文件整合，再處理 R01 的同交易 baseline/current family 重讀假設。
入口為 [analysis repository](../src/h2hdb/vnext_analysis_repository.py) 的
`_require_file_decision_targets`／`_load_file_decision_evidence` 與
`vnext_analysis_decision_batch.py` 的 shadow/tombstone exact-family loaders。
先確認是否能以一次有界讀取推導兩個結果且保留 partial/orphan family、shadow+tombstone、
missing layout、checkpoint/replay 的拒絕契約；不得跨 issue／prepare／commit 快取 authority。
若要作效能實驗，先從既有成本契約固定 SQL／rows examined、local CPU／memory 與 elapsed
預算，列明既有資料量、本次工作量、hash 重用率、artist 密度分布與 overlay depth。
涵蓋 genesis、一般 overlay、policy-change／depth-16 compaction、頁容量前／上／後與
重複循環，並以保留重讀的退化反例驗證計數；SQLite 與 MariaDB 分開實測。
未建立固定預算或取得中大型證據時保持待實驗，不宣稱收益。
雙 `page.verify()` 是次要規則收斂候選；R02 及 S2／S3 的其他候選仍需後續審核。
此單元不代表 C01–C04 已審核完成，也不預設拆包或一定有程式可刪。

## 每輪交付與證據

- 一個內聚候選的問題、必要責任、行為變化、假設、反例、採用或不採用理由。
- 涉及修改時，記錄實作 commit、影響範圍及實際執行的既有檢查；未跑、skip 分開列明。
- 效能主張附事前成本契約、中大型資料形狀、固定／逐筆成本及 baseline／candidate provenance。
- Ledger 只保存摘要和證據索引：repo／ref、相對路徑或 artifact ID、hash、命令與結果。
  大型 logs 不複製進本文；缺失證據記為待重新取得，正式 receipts 不由本文取代。
- 本輪完成前回寫候選、範圍審核狀態與下一個入口；只改 ledger 不代表 runtime 通過。
  實作 commit 可先記錄，最後 merge 由 Git history 核對，避免自我引用 commit hash。

預設一輪完成一個候選的完整處置：先說明具體修改方案、預期收益及驗證方法，
完成相稱的調查／實驗；證據支持採用時接續實作、必要驗收、整合及受影響範圍重查。
充分證據支持拒絕或保留時，可附理由與重開條件結案，不強迫修改或刪碼。
調查是候選內的步驟，列出下一步、提交文件或上下文壓縮都不是正常停止條件；
有資料、能力與既有授權可繼續時，接著完成待辦，不把「尚未驗證」當成結案。
使用者明確要求只分析或查進度時，依指定範圍交付；因實際阻礙或使用者限制而
中斷時，保存已完成工作、具體阻礙及恢復入口，保持未完成狀態。
待整合、待實驗、待決策或缺少必要證據的候選仍未完成；單一候選結案也不代表
其所屬範圍或全系統已審核完成。既有授權內的工作不逐步要求再次確認。

可重用 [驗證說明](../verification/README.md)、[語意證據索引](../verification/invariants.toml)、
[Core SQL 成本工具](../scripts/check-ingest-database-performance.py)，以及 Ingest 的
`scripts/check-source-cost.py`、`scripts/check-library-cleanup-cost.py`。
既有規則適用的 gate、人工驗收與 review 仍由各 repo scripts 負責；本文不新增 gate。

## 全系統完成條件

本計畫涵蓋上述六個套件、自有部署控制來源與工程工具；不代表全庫已達理論最簡。
各階段以自己的範圍與證據結案；全系統完成另外需要以下條件。

1. 每個 C／I／O／K／D／H／P／E 分組及接續期間新增的自有模組、工具與角色
   都有審核記錄，無未分類缺口；缺少 workspace、source 或驗收證據時尚未完成。
2. 每個發現都有可追溯處置；延期須列原因、重新開啟條件及尚未達成的目標。
   未完成的範圍內必要工作不能用延期標記充當完成；範圍變更另記明確決策。
3. 採用項目已整合並完成其必要驗證；程式正確、成本達標、部署可用分別記錄。
4. 在所有 repository 的最終 ref 及部署控制檔 hash 上重查功能責任、重複工作與耦合，
   包含未啟用 jobs、共用 build 與跨庫檔案／鎖契約，沒有遺漏的可處理範圍內問題。
5. 拆包各自有採用／拒絕／後續階段決定與依據；拆包數量和 LOC 減少都不是完成指標。

目前進度：範圍已擴至全系統；R01 file validation step 的責任／狀態調查完成，
保留三階段並列出兩個待驗證局部候選。其餘 runtime／部署審核與工程工具細分仍待做；
R01 整體、S1–S4 及全系統完成條件均尚未滿足。
