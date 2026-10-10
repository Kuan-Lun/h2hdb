# H2HDB 全系統分割與精簡任務紀錄

這是跨對話接續用的任務範圍、候選與驗收狀態，不是第二份代理政策。
政策以各 repository 的 `AGENTS.md` 為準；Core 入口為 [AGENTS.md](../AGENTS.md)。
目標是六個自有程式 repository 與部署 workspace 的內部責任分割與整體精簡：
集中完整功能的 ownership，隨相關分割刪除已確認的重複實作，不以先證明大量減碼
作為開始分割的前提。分開記錄 runtime、tests、
dev tooling 與 generated code 的增減；LOC 不是唯一指標，但不能以搬檔或加速冒充減碼。
優先在正確 ownership 的 repository 內建立內聚子套件；只有獨立使用／發布邊界
和淨收益成立時才考慮 PyPI 拆包，不把相同 schema、交易或生命週期切成多個發行單位。
2026-10-09 使用者將原 Core／Ingest 計畫擴至整套系統，其他 workspace 也需主動審核。
範圍擴充與 R01 同交易 family 共讀已整合；R01 是效能改善，runtime 淨增 30 行。
R02 共用 session／page validation 已整合，runtime 淨減 83 行。
2026-10-10 使用者要求提高每輪的實質維護收益；不再機械式優先處理 R07 小型 helper。
R15 收斂完整 CBZ 解析責任並建立 Ingest 內部 artifact 子套件，驗收狀態見下節。
第二次 page verification 經故障反例確認保留；全系統審核與模組邊界決策仍未完成。

## 目前任務快照

2026-10-10：使用者確認主要方向為專案內部責任分割，並要求檢討新對話交接。
本輪完成最新 log 分析、七個 workspace 來源重查與接續流程修正；不啟動大型
runtime 搬移。下一輪以 **Core cleanup 的完整內部責任分割** 為主要交付，
細節見「下一輪入口」；R03 的效能定位另行驗收。舊 R 編號是證據索引，不是執行順序。
Analysis／R16 保留但順序後移；不要重回先找小 helper 或以減行數篩選所有工作。

新對話使用 `$h2hdb-simplify`，其顯示名稱改為「h2hdb 分割與精簡」。先讀本快照、
下一輪入口及範圍表，再按責任查閱歷史實驗。Skill 負責接續流程，本紀錄保存
當前狀態，原 `AGENTS.md` 與 executable checks 仍是各庫政策來源。
新對話不保證自動保留舊聊天；不可用聊天記憶代替上述來源。

本輪開始時六個主要 checkout 均乾淨，refs／versions 為：

| Repository | 已核對 HEAD | Version | Runtime 模組／實體 LOC |
| --- | --- | --- | --- |
| Core | `c583a8979948a6e857bf9f78ffeccdba96738a7c` | `0.45.8` | 96／117,031，含 generated 24 |
| Ingest | `3ce26cd8e7c19ab0db7df15b2828c801efb48356` | `0.30.4` | 53／20,006 |
| OPDS | `03be557ebd78763b74288f7c39a9e4934934148b` | `0.24.2` | 23／6,384 |
| Komga | `0182984a1357f5a1345d3dd78ff8dca94f0cf597` | `0.18.2` | 7／1,511 |
| Downloader | `e6fbe969a3300dd90bb4174fa3e08e7e7b4e3053` | `0.23.2` | 3／1,367 |
| hbrowser | `b8bf1ba306f3222ca12cb589d4d10e08e7a25fc3` | `0.44.2` | 49／18,827 |

六庫非 generated runtime 合計 165,102 行，比初始盤點少 138 行；這是 R01、R02、
R15 的累積結果，不是本輪刪碼。R01 `e9422d7`、R02 `d375614`、Core R15 `1010ec2`
及措辭澄清 `eaed02d` 均為 Core HEAD ancestors，Ingest R15 `3ce26cd` 即其 HEAD。
Core HEAD 的 documentation receipt 有效；歷史 full gate 結果見 R15，不冒稱本輪重跑。
部署仍無 Git，其 23 個自有控制檔、5,598 行及下節 SHA-256 重算未變；未讀 secrets
或查執行中 images。Core `formal/close-production-blockers` 與 Ingest
`feat/page-worker-decision-log` 的既有 branch／worktree 保留，沒有接管其修改。

本輪以品質優先修正接續入口，不受最小修改或向後相容限制；僅有任務文件與個人
skill 變更，沒有 runtime 介面、資料格式或 compatibility path 變更，屬「品質優先後
恰好向後相容」。Core 維持 `0.45.8`、impact none、依賴未變，不重跑 dependency audit。
Runtime／tests／dev tooling／generated code 的本輪增減均為零。提交與整合依
documentation profile 驗證，不將它當作 runtime、效能、Compose 或 production 驗收。
個人 skill 的 `quick_validate.py`、UI YAML 欄位檢查及 repository-local Markdown
檢查通過；已安裝並逐 byte 核對兩份來源。沒有舊聊天背景的唯讀接續演練正確選到
cleanup 分割、原驗收邊界與剩餘事項；這是交接可讀性驗證，不保證未來代理不會偏移。

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

以上為歷史盤點。R02 的 Core 基線為 `e9422d788217a25ef56fefea3b25946670e458f0`
（`0.45.7`），已含 R01 實作、code review 與有效 full release receipt；其他五庫
仍為上表 ref。六個主要 checkout 開始時乾淨，既有非本任務 branch／worktree 保留。
R02 新增兩個 private Python 模組歸 C04，Core 模組由 94 增為 96；新增目錄本身
不算減碼成果，完整 source delta 見 R02。前次 LOC／部署摘要保留為歷史基線。

R15 基線：Core `d37561428c0338ec9a475c1a637c237ce1c94e9e`／`0.45.8`，
R02 實作 `9039d9d`、升版 `0848ba5` 已合併，重新核對 full receipt 有效；其餘五庫
HEAD 與上表相同。六個主要工作樹開始時乾淨，既有其他分支／worktree 保留。
Ingest 的 `artifact.py` 改為六個 package modules，48→53 模組，全歸 I02；
本輪 runtime LOC 為 20,006，Core runtime 117,031（含原 generated wrapper）。

## 已知與未證實

- 已知：上述歷史差異、模組盤點及 R02 的相同函式 body 可由 Git source 核對。
- 已知：codecs 有純函式邊界；schema、cleanup、fencing 有跨表與交易耦合。
- 已知：R01 file validation 的 local plan 已跨頁重用；issue／prepare／commit
  分別擁有 durable coordinates、local preparation 與 fresh commit authority。
- 已知：R01 同呼叫 ancestry 去重可保留 family 拒絕契約，且在下述本機中大型
  重疊案例降低局部 SQL 工作與時間；移除第二次 page verification 的反例失敗。
- 未證實：整體狀態數可減少、拆包有淨收益或整個 ingest／部署達成成本目標。
- 先前聊天與暫存 logs 只提供線索；量化結論須重新取得有來源版本的持久報告。
- R01 實驗、受影響 Core／Ingest 測試及局部範圍複查結果見下方；不可外推為
  全系統 runtime 審核或 production 驗收。

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
| S1 | Core／Ingest 的內部責任分割與相關精簡 | R01／R02／R15 已整合；下一單元為 Core cleanup，其餘範圍未完成 |
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
分組原基線覆蓋 94／48／23／7／3／49 個模組，無重複或遺漏；下表 C04 已納入
R02 的兩個新模組，Core 現為 96；I02 納入 R15 的六個 artifact modules，Ingest 為 53。
C02／C04 與 I04 已局部追蹤 R01 呼叫路徑，
C01／C04 補入 R02 驗證邊界；所有分組的完整審核仍未完成。
新增模組或 HEAD 變動時，差異仍待分類；候選表不取代完整範圍清單。

| ID | 功能範圍 | 數量 | 模組模式 |
| --- | --- | --- | --- |
| C01 | Source、observation、staging | 16 | `vnext_source_*`, `vnext_gallery_staging_*`, `vnext_gallery_identity_repository`, `vnext_manifest_family` |
| C02 | Analysis、decision、hash reuse | 8 | `vnext_analysis_*`, `vnext_file_decision_validation_plan`, `vnext_hash_cache_repository` |
| C03 | Publication、artifact、activation | 10 | `vnext_publication_*`, `vnext_artifact_*`, `vnext_library_activation_repository` |
| C04 | 公開 orchestration／facades 與 private ingest validation | 6 | `vnext_ingest_facade`, `vnext_ingest_analysis`, `vnext_ingest_publication`, `vnext_facade`, `_ingest/*` |
| C05 | Cleanup、canonical persistence | 5 | `vnext_cleanup_*`, `vnext_canonical_value_*`, `vnext_canonical_consumer` |
| C06 | Coordination、queue、transaction | 10 | `vnext_allocator_repository`, `vnext_download_ingest_repository`, `vnext_ingest_fence_repository`, `vnext_ingest_policy_repository`, `vnext_maintenance_gate_repository`, `vnext_operational_event_repository`, `vnext_queue_repository`, `vnext_storage_instance_repository`, `vnext_transaction`, `vnext_state_machine_contract` |
| C07 | Schema、validators、audit、capacity | 12 | `schema_*`, `vnext_schema_provider`, `_generated_vnext_schema`, `_schema_artifact_codec`, `catalog_refinement`, `operational_refinement`, `source_collection_refinement`, `catalog_writer`, `database_audit`, `vnext_physical_domains`, `vnext_capacity` |
| C08 | Codecs、domain、ports、errors | 9 | `vnext_identity`, `vnext_domains`, `catalog_search`, `domain`, `ports`, `artifact_errors`, `artifact_resources`, `catalog_errors`, `source_errors` |
| C09 | Catalog read／identity／registry | 3 | `vnext_catalog_*` |
| C10 | DB transport、pool、composition、clock | 6 | `*connector`, `mariadb_pool`, `repository`, `database_clock` |
| C11 | Telemetry、logging | 6 | `*performance`, `ingest_performance_format`, `logger` |
| C12 | 啟動、設定、exports | 5 | `__init__`, `__main__`, `config_loader`, `environment`, `settings` |
| I01 | Source observation／scheduling | 5 | `core_source`, `filesystem`, `source_monitor`, `source_schedule`, `_source_retry` |
| I02 | Media／qualification／artifact policy | 11 | `artifact/*`, `artifact_errors`, `image_qualification`, `page_workers`, `policy`, `source_image` |
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
R01／R02／R15 已整合，2026-10-10 已核對 Git ancestry；R15 為單一 archive parser
與內部責任分割。下列歷史候選以目前任務快照及下一輪入口決定優先序。
其他 analysis stages 與其餘範圍仍未完成。整合狀態由 Git ancestry 與 receipt 核對。

| ID | 候選／狀態 | 持久證據入口 | 結案或重開條件 |
| --- | --- | --- | --- |
| R01 | 同交易 family 共讀已整合；第二次 verify 保留，移除候選拒絕 | C02／C04、I04；下方實驗、故障反例與持久報告；merge `e9422d7` | 相關 authority／plan ownership 改變或新工作負載違反局部契約時重開；其他 analysis stages 待審核 |
| R02 | 相同 session／page predicates 共用；已整合 `d375614` | C01／C04；`_ingest/validation.py`；下方雙 backend 與 consumer 驗證 | 各信任邊界保留驗證；authority 欄位、分頁語意或 caller lifecycle 改變時重查 |
| R03 | Cleanup 成本定位優先；READY audit 分開處理，兩者未完成 | C05、C07；R01 成本違反與本輪 log-4 證據；下一單元先建立 cleanup 內部責任邊界 | 沿用既有預算定位可省工作；分割或 READY audit 通過不能覆蓋 cleanup 成本失敗 |
| R04 | 純 codecs distribution；延期至邊界審核 | C08；`vnext_identity.py`、`catalog_search.py`、`catalog_writer.py`、`database_audit.py` | 先確認 byte／Unicode／guard ownership／audit 失效契約；邊界與收益成立後再決定拆包 |
| R05 | Media distribution 暫不採用；R15 先完成內部分割 | I02 與 I01／I03 使用點；Ingest `artifact/` | 有獨立使用／發布需求及可降低耦合的證據才重開 PyPI 拆包；現有 renderer 仍接 source authority、storage 與 Core evidence |
| R06 | Transport／telemetry／contracts；延期 | C08、C10、C11、I05；實際 import 與 consumer 使用點 | 有獨立使用需求、可減少依賴或發布耦合的證據再開；檔案大小不是理由 |
| R07 | Library／relocation 局部重複成立但收益較小，降低優先序 | I03；marker JSON 與 quarantine hash framing 相同 | 應併入完整 library I/O ownership 工作；保留 relocation object guard／path validation、exact-prefix restore 與 runtime atomic write 的差異，不為小 helper 單獨啟動下一輪 |
| R08 | OPDS／Komga reader fencing 共用規則；待審核 | O04、K02；OPDS `library.py` 與 Komga `coordination.py` | 比較開檔機制、錯誤分類與持鎖期限；request 與整次 sync 的生命週期不能直接等同 |
| R09 | Downloader root／batch 工作生命週期；待審核 | D02–D03；`downloader.py` 的 `_run_coordinated_batch`、`_run_coordinated_root` | 對照 heartbeat／handoff 與不同 completion 邊界後，判定共用規則或保留；契約變更時重開 |
| R10 | Browser ownership／deadline／diagnostics；待審核 | H01–H06；`gallery/browser/`、`gallery/utils/` 及 driver 呼叫者 | 先驗證狀態與資源所有權；兩次 fresh empty 的 confirmed-missing 證據不能視為無用重讀 |
| R11 | OPDS 協定與 catalog mapping；待審核 | O01–O05；`opds12.py`、`opds2.py`、`atom.py`、`serialization.py` | 分清協定差異與重複映射責任，再決定可共用部分；協定差異本身不是冗碼 |
| R12 | 部署解析／啟動／驗收／操作責任；待審核 | P01–P07，包含全部 profile jobs 與共用 build stage | 驗證 resolver、啟動 probes、隔離驗收與 deploy 各自的責任；不得以讀過來源宣稱可部署 |
| R13 | 跨庫工程工具收斂；待逐檔盤點 | E01–E02；既有 scripts、hooks、workflow 與驗收 fixture | 先比較執行語意與 ownership，列出真正重複者；不能以抽共用套件取代各庫應有驗收 |
| R14 | Source append SQL 成本；已重現超標、待定位 | C01／C04；R01 完整流程的 source attribution | 沿用既有 source 預算，固定代表形狀與退化反例，驗證重複工作後採用或拒絕 |
| R15 | Canonical CBZ 單一解析與 artifact 內部分割；完成已整合 | I02、E02；Ingest `artifact/`、Core deployment acceptance probe；下節驗收 | 格式、writer 或 source/cache 邊界變更時重查；不另建平行 presentation package |
| R16 | 完整 analysis 內部 ownership；待實作，順序後移 | C02／C04；`vnext_analysis_repository.py`、overlay families 與 facade 編排 | 四組 BUILD／VALIDATE／replay 隨各 family 集中；保留各信任邊界，不用大量 flags 掩蓋差異，不以先減碼為門檻 |

## R01：file decision validation step 調查（歷史紀錄）

本節為 `413d06a` 的調查快照，已經 `041f707` 整合；其中「待驗證」與
「未執行」是該輪狀態，後續實驗及處置以下方 2026-10-09 結果為準。

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

以下為整合前的實驗紀錄；後續已以 `e9422d7` 合併至 `master`，Core `0.45.7`，
code review 與 full release receipt 有效。文內「待整合」不是目前阻礙。

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
- 涵蓋 genesis、baseline 1／8／16 layers 的 overlay、policy-change self-only、
  baseline 17 layers 的 depth-16 compaction；34-layer 非 suffix 邊界只作有界
  正確性案例。Inherited-value reuse 分別 0%／95%，不得混稱跨 gallery hash 重用率。
  本 fixture 使用已聚合 artist scalar 分布，並不冒充完整 source artist 分布；
  故結果只支持 family 讀取熱點，public pipeline 正確性另由既有測試驗證。
- SQLite／MariaDB 分開量測，在同資料上交替 baseline/candidate，各至少三次完整循環。
  報告固定 SQL/native work、K=1 的固定加單筆成本、逐 key 正規化 local CPU／elapsed，
  以及 Python peak memory；不從三次計時推定獨立的固定 CPU 截距。
  本機新增的回歸預算為每次 pair Python peak ≤ 8 MiB、CPU ≤ 1 ms/key、
  elapsed ≤ 5 ms/key；它們是本候選的固定驗收預算，不是既有全管線 SLO。
  中大型重疊形狀的 candidate median wall 必須低於 baseline，才能宣稱本機淨收益。
  非重疊案例記錄新增成本，不能以漸近改善或小型查詢減少代替主要規模實測。

本節建立時尚未量測或採用實作；結果與持久證據於同一任務內補入。

補充形狀（執行前固定，原預算不變）：原 fixture 每 key 在 baseline ancestry
只存一份 family，另量測 4,096 retained／requested keys、8 個 baseline layers
每層每 key 都有完整 shadow 的高歷史改寫密度，current inherited reuse 為 95%／0%。
另以兩個不重疊的 17-layer roots、128 keys、每層完整 family 檢查最大聯集的
Python peak 與局部 CPU 成本。此補充只支持已測密度／規模，不冒稱 32,768 dense
keys 或完整 source 分布已驗證；仍用三循環及同一原始 baseline 比較。

### 實驗結果與處置

採用同呼叫 family 共讀：`_load_file_decision_evidence` 接受一或兩個 roots，
各自 fresh 讀 layout，將 ancestry 去重後以既有 17-layer loader 分批讀取，
再按每個 root 的順序獨立解析。Policy self-only、compaction 與不相交 34-layer
聯集不假設 suffix；hidden ancestors 的 partial/orphan family、shadow/tombstone
衝突仍 fail closed。呼叫結束即丟棄共用資料，不新增 durable state 或跨交易快取。
兩個直接呼叫點共用同一 helper，刪除只轉傳 `.resolved` 的 wrapper；runtime 淨增
30 行，換得消除重複 I/O 及單一有界讀取實作，不宣稱 LOC 減少或完成拆包。

歷史 baseline 為 `041f707` 的精確 helper AST，實驗與 candidate 使用相同共用
dependency source；報告另附 `_load_layout`／value types／loader module 的來源
比對。這不是整個歷史部署映像的 wall-time 比較。計時只含 pair helper 的 layout、
family SQL、解析及結果建構，不含 transaction 進出、獨立 oracle 或圖片處理。

| Backend／95% inherited shape | Baseline median | Candidate median | 減少 |
| --- | --- | --- | --- |
| SQLite，4,096 keys／baseline 8 layers | 0.28031 s | 0.15145 s | 46.0% |
| SQLite，32,768 keys／baseline 16 layers | 4.45069 s | 2.29836 s | 48.4% |
| MariaDB，4,096 keys／baseline 8 layers | 0.64048 s | 0.34832 s | 45.6% |
| MariaDB，32,768 keys／baseline 16 layers | 8.33179 s | 4.32066 s | 48.1% |

0% inherited 的主要案例也降低約 44–48%；高歷史改寫密度的 4,096-key 案例
SQLite 為 0.39631→0.20961 s，MariaDB 為 0.74238→0.41571 s（95% inherited）。
4,096／32,768 keys 的 family SQL 各由 128→64／1,024→512；grid rows 各由
139,264→73,728／2,162,688→1,114,112。固定 128 keys 在兩種 retained 規模的
native work 相同。最大 34-layer dense pair 的 Python peak 為 1,891,484 bytes，
未超過 8 MiB 預算；所有採用案例的固定 CPU／elapsed 預算及主要重疊收益通過。
Peak 是當次 `tracemalloc` Python allocations，不是 process RSS 或 MariaDB server
memory；baseline 為獨立 oracle 保留兩份完整 evidence，歷史 caller 僅保留
`parent.resolved`，因此不是精確的歷史 commit 記憶體比較。
非重疊不保證加速：MariaDB compaction 129 keys 為 19.085→19.327 ms，增加
0.242 ms，仍在預算內；此處沒有可省的重複 family coordinates。

在此讀取契約下，family coordinate 工作由 `K*(B+C)` 降為 `K*U`；例如
16／17 layers 重疊時剩下 `17/33`，不是宣稱所有 SQL 演算法的絕對理論下限。
實測重疊案例從 B=1、K=1 已有淨收益，但未推論 NAS 的損益平衡點或整個 ingest
速度；未測 32,768 dense keys、完整 source artist 分布或正式部署資料。

持久證據為 [摘要](../benchmarks/r01-file-validation.json) 與
[原始報告及精確 runner snapshots](../benchmarks/evidence/r01-file-validation.tar.gz)。
Archive SHA-256：`20d727189e616a44c6f3af7f4224a9b0622339eb21bf546a8810dbdc32892f14`。
包含 9 reports、4 runners、34 採用成本案例及 2 個強化 oracle smoke；摘要列明
每份來源 hash、採用 case index 與排除理由。早期 pilot 不用於上述結論；混合報告
中已替換的邊界案例保留原始資料但不採用。可重跑工具為
`scripts/ingest_file_validation_cost_probe.py --help`，預設歷史 ref 綁定本次 baseline；
它是開發期 R01 成本實驗，不是 production compatibility path。
退化反例包含恢復舊 double-read 及讀取錯誤 owner set；後者即使 rows／owners
數量相同也必須被獨立 oracle 拒絕。
新增成本工具及其測試歸 E02；沒有新增 runtime module。實作 commit `d99f5c9`，
成本工具／證據 commit `bbb4d2d`；本節及手動驗收報告在後續提交保存。

**拒絕移除第二次 `page.verify()`**：兩次呼叫之間 `_prepare_batch` 可能等待 DB
authority，owner plan 可被關閉或 metadata 改變。新增 SQLite／MariaDB fault test
在該間隙注入變動，要求拒絕且 checkpoint 不前進。只 bypass binding helper 的
第二次 verify、保留其他驗證的 negative control，兩個 SQLite 案例都以
`DID NOT RAISE` 失敗，證明目前重複檢查有必要。若未來 plan ownership／immutability
能覆蓋這段等待，才重開此候選；本輪保留 issue／prepare／commit 邊界。

### 驗收與影響範圍

Core targeted 命令以 `.venv/bin/python -m pytest` 執行；非 MariaDB 與 MariaDB
均指定 `-n 0 --check-backend-pairs`，後者另外設定 `H2HDB_TEST_MARIADB=1`。

- `tests/test_vnext_analysis_decision_batch.py tests/test_vnext_ingest_analysis_validation.py`
  配 `-m 'not mariadb' -q`：58 passed；配 `-m mariadb -q`：53 passed。
- 加強 genesis／next-transaction fresh evidence 後，decision batch 檔配
  `-m '' -k paired_evidence_preserves -q`：16 passed；新增最大聯集／空 keys 後，
  配 `-m '' -k 'disjoint or empty_evidence_keys' -q`：5 passed。
- `tests/test_ingest_file_validation_cost_probe.py -n 0 -q`：1 passed，驗證錯誤
  owner-set negative control；targeted strict mypy、Ruff／format 通過。
- Negative control 為暫時 monkeypatch class verify，僅當 caller 是
  `_require_file_validation_binding` 才 bypass；analysis validation 檔配
  `-n 0 -m 'not mariadb' -k test_commit_rechecks_plan_after_database_authority_wait -q`
  取得預期的 2 failures；沒有將退化實作寫入 runtime。

候選 wheel 為 Core `0.45.7`，SHA-256
`929de5c18be464c67feeca59df7b916b510dd3672f3486e3bd87bf90c939c94a`；
Ingest `0.30.3` wheel 的 49 package files 與 `c5a6967` source 一致，SHA-256
`a26e81530f7e2017c9e97bf0db194d0fc05428e266ebae40050515f4c5436ba7`。
乾淨 venv 以正常 dependency resolution 安裝兩個明確 wheels 與 Pydantic `2.14.0`，
`uv pip check` 通過；實際 imports、METADATA 與 installed payload 分別比對 wheel
96／49 files。`direct_url.json` 未提供 archive hash，另以實際 bytes 計算核對。
Ingest 以此環境執行 `tests/test_service_vnext.py tests/test_session_vnext.py
tests/test_config.py`（`-p no:cacheprovider -o addopts= -n 0`）：242 passed、無 skip。
Service／session 使用 fakes，不能當作 live DB 證據；Core 的雙 backend 測試另列。

完整 pipeline 成本另以 `scripts/check-ingest-database-performance.py`，在兩個
backend 各跑舊版 `041f707` 及 candidate。參數為 `--backend sqlite` 或
`--backend mariadb --allow-mariadb`，均配 `--case 128:128:1 --replacement-case 4:2
--output REPORT.json`。四次均完成量測、READY audit 成本通過，但 pipeline 成本
**失敗、exit 1**：source append SQL 都為 160,432，超過既有上限 137,216；
replacement cycle 2 cleanup 上限 3,152，舊版兩個 backend 及 candidate MariaDB
均為 3,182，candidate SQLite 為 3,184。沒有調高預算或將工具成功量測當成達標。

SQLite cleanup 的額外兩次 SQL 精確來自多重開一個 COMPLETE shard job 的
frozen-root SELECT 與 job DELETE；READY audit 也觀察到 33 而非 34 個 job rows。
此 fixture 的 random analysis ID 會影響 shard 分布，是與程式一致的原因解釋；
報告未保存 SQL parameters，無法指認確切 target。R01 修改只讀取 family，沒有
改變 cleanup 寫入／shard 規則；上述超標已在 baseline 重現，保留為 R14／R03。
這些 pipeline 與手動清理曾併行執行，只比較 SQL 工作及既有契約，不據 wall time
宣稱端到端加速。128-gallery 回歸也不取代下一候選應有的中大型效能驗收。

手動 `.venv/bin/python scripts/run-pytest.py cleanup-acceptance` 完成，exit 0：
SQLite 259 passed（331.6 s）、MariaDB 10.11.11 242 passed（1,833.5 s），
涵蓋真實 compaction、深度邊界與清理／稽核恢復契約。它不是完整 deep suite，也
不屬於 bounded merge receipt。四份完整 pipeline 報告、來源與 SQL 差異核對、
手動清理 stdout、consumer 的解析／metadata／測試紀錄及本輪 installed skill
snapshots 保存於 [手動驗收索引](../benchmarks/r01-manual-validation.json) 與
[原始證據 archive](../benchmarks/evidence/r01-manual-validation.tar.gz)，SHA-256：
`f694e42601b6714738111f4efedaa8abdf1bdf753e547e78a59f33ae4870808c`。
每個 archived input 另有 byte size／hash；不把這些手動報告當成正式 release receipt。

重新盤點實際 Compose、Dockerfile 與 resolver 的四個 role，candidate lane 符合
既有範圍；本輪只改 Core private helper，沒有公開 facade、schema、資料格式、
telemetry、dependency range 或部署控制變更。下游實作不需修改；受影響的 Ingest
orchestration/session 已重查並驗證。未執行 OPDS／Komga／Downloader 的無關完整
suite、實際 Compose build／隔離發布流程或 production deploy；candidate wheels
取代了 index releases，不能因此宣稱正式 index-backed Compose 已可部署。

品質優先且不以最小修改或向後相容限制設計；採用同呼叫去重並保留必要 authority
檢查，沒有跨交易 cache 或新增套件。結果為「品質優先後恰好向後相容」，沒有
compatibility path 移除。Runtime shipped surface 需 patch 升版至 `0.45.7`；
dependency audit 已重新產生並審閱，最新 Pydantic `2.14.0` 的 release notes 及
四個 config tests 通過，final gate 的環境亦已更新至該版本。局部計時使用之前的
Pydantic `2.13.5`；family helper 不使用它。其他直接依賴最新版與既有 audit 相同，
未改依賴宣告或 upper bound。正式 code review／full merge gate 尚待整合執行，
由 Git metadata receipt 核對，不預寫通過；未 push、publish 或 deploy。

Skill 也已修正並安裝：預設完成候選的實驗、採用／拒絕、必要實作與驗收整合，
不能以 docs commit、列下一步或上下文壓縮停止。Skill schema、Markdown、Codex
重新載入與獨立情境檢查通過；使用者明確只要分析仍不自動改碼，充分保留證據
也可結案。Skill 是使用者環境的任務流程，沒有複製 repository 開發政策。

## R02：共用 ingest validation

基線為 Core `e9422d7`。使用者要求實際減少重複程式，並優先使用 repository 內部
子套件；本輪不延伸 R01 效能工具或新增 PyPI 發行單位。採用
`src/h2hdb/_ingest/validation.py` 作為規則唯一 owner，四個既有 callers 直接引用。
Session 比較與 page cursor 規則同屬 ingest 邊界驗證；page 規則需要上頁 cursor、
capacity 與 component context，不是 neutral domain value 本身的約束，因此沒有
擴大 `domain.py` 或新增公開 API。Facade／spool 的交易和生命週期仍由原模組負責。

| 項目 | 之前 | 之後／處置 |
| --- | --- | --- |
| Session authority identity／guard | source、analysis、publication 各一份 | 各只留一份共用實作；保留 source／analysis issue replay 與 commit，以及 publication commit 呼叫 |
| Named／tag page validator | facade 與 observation spool 各一份 | 各只留一份；adapter freeze 與 frozen-page replay 各次仍驗證 |
| 相同規則的函式數 | 10 | 4，刪除 6 份重複實作，不留下 shim 或 forwarding wrapper |
| Runtime Python 實體行數 | 相對基線 | 淨減 83，包含新增 `_ingest/__init__.py` 與 validation module |
| Tests Python 實體行數 | 相對基線 | 淨增 80；Python 合計淨減 3，不將測試排除後冒稱總減幅 |
| Dev tooling／generated Python | 相對基線 | 都未變；未新增實驗 runner 或報告 archive |

以 `git show e9422d7:<path>` 與 AST 比較，三份 session identity、兩份 named page、
兩份 tag page 的函式 body 與共用 owner 一致。Guard 只將原 error prefix 改為明確
`step` 參數；九個 authority 欄位、兩個 lease expiry 的排除、`__post_init__()`、
容量、排序、terminal 與 cursor 規則均保留。這是 source 等價檢查，runtime 契約
另由下列測試驗證；沒有宣稱 SQL 次數、端到端時間或清理成本改善。

受影響測試檔為 `tests/test_vnext_ingest_public_contract.py`、
`tests/test_vnext_ingest_analysis.py`、`tests/test_vnext_ingest_publication.py`、
`tests/test_vnext_source_preflight_manifest.py`。前兩者補強 renewed session 的 issue
replay／commit 與五個 gate／ingest authority 欄位偽造；publication fake 驗證全部
九欄及 linked→unlinked 偽造，不能把此 fake 當成 live DB 證據。新增六種 page 案例
涵蓋 FILE／DIRECTORY／TAG × adapter／replay，錯誤 cursor 必須在 durable staging
checkpoint／request／receipt 變更前拒絕，失敗 source handle 也不能重新使用。

上述四檔以 `.venv/bin/python -m pytest -n 0 --check-backend-pairs -q` 執行：
`-m 'not mariadb'` 為 90 passed／51 deselected（37.87 s）；
`H2HDB_TEST_MARIADB=1` 配 `-m mariadb` 為 51 passed／90 deselected（169.05 s）。
沒有 skip；每個新增可攜 page 案例均在兩個實際 backend 執行，database snapshot
也使用所選 backend。Targeted Ruff、format、strict mypy 與 `git diff --check` 通過。

重新讀取實際 Compose／Dockerfile／resolver 及四個 consumer dependency range，
候選 Core `0.45.8` 在現有 `>=0.43.0,<0.46.0` 內。公開 facade、immutable values、
schema、protocol、telemetry、dependency range 與平台支援未變；只有 Core 實作與
測試需修改，Ingest 是直接 consumer。

乾淨 venv 以 `uv pip install --refresh --python VENV/bin/python CORE_WHEEL
h2hdb-ingest[dev]@file://INGEST_WHEEL` 正常解析 43 packages，未用 `--no-deps`；
Core `0.45.8`、Ingest `0.30.3`、Pydantic `2.14.0`、galleryinfo parser `0.5.3`，
`uv pip check` 通過。Core wheel SHA-256：
`93aa60ef69e987d246e2cbe2ead88c4231930aa79e538b2498254d96a8fa7fff`；
Ingest wheel SHA-256 與 R01 相同，49 package files 仍與 `c5a6967` source 一致。
Core 98／Ingest 49 package files 的 wheel／source／installed bytes 和 METADATA
均核對，兩個新 `_ingest` 檔案已包含；`direct_url.json` 的來源亦核對。
在 Ingest checkout 清除 `PYTHONPATH` 後，以此 venv 的 `python -B -m pytest
-p no:cacheprovider -o addopts= -n 0 -m 'not deep'` 執行 `tests/test_service_vnext.py`、`tests/test_session_vnext.py`、
`tests/test_config.py`、`tests/test_filesystem_vnext.py`、`tests/test_source_reread.py`：
300 passed、0 skip、1 deep deselected（3.32 s）。前兩檔是 fake Core 邊界，後兩檔
涵蓋實際 filesystem adapter 的 named／tag pages 和 255／256／257 容量，不是 live
Core SQL 證據。這個明確 candidate wheel 取代 index Core；不據此判定 index 發布狀態
或宣稱正式 Compose 已可部署。測試可由上述 refs、wheels 與命令重建。

本次以品質優先選擇單一規則 owner，未以最小修改或向後相容限制設計；結果為
「品質優先後恰好向後相容」。沒有公開行為或資料格式變更、沒有 compatibility
path 移除；刪除的是未公開的重複 helper，沒有相容 shim。Shipped runtime 變更需
patch 升版 `0.45.7`→`0.45.8`。重新查詢 registry 並人工審閱 dependency audit：
所有 Python／Node 直接依賴最新版與前次 receipt 相同，未改任何依賴宣告。

本節寫入時仍待正式 code review／full merge gate；以本節的實作 commit、Git
ancestry 與 exact-tree receipts 核對完成狀態，不預寫通過。沒有 cleanup、compaction、
交易快取或部署組合變更，因此不重新執行 cleanup-acceptance、效能基準、完整 deep、
Compose build／隔離發布流程或 production 驗收；本輪不宣稱部署可用或效能達標。
下次只在相關 source 變更時重開 R02，不將已消除的重複規則再列成待調查項目。

## R15：artifact 完整責任收斂與內部分割

使用者要求避免反覆完成小型 helper 後，重新比較 Core analysis、Ingest library 與
artifact。沒有證據支持可直接安全刪掉數千行；library／relocation 的 marker 重複
較小，且 hardlink healing、重新 hash、atomic replacement／exact-prefix continuation
具有不同權威，因此 R07 降序。本輪選擇完整的 CBZ inspection：原本 raw parser 已
驗證 central／local fields，接著 `ZipFile` 又建立 member state、重讀 header 並驗證。

Ingest 基線 `c5a6967`，實作 `a92747c`、升版 `e3e6463`。以單一 bounded raw parser
輸出 `_CanonicalMember`，直接驗證 metadata DEFLATE 與 page JPEG／CRC；刪除第二套
`ZipFile` reader、`ZipInfo` map、重複 local-layout／offset helpers、排序 overlap pass
及可由連續 canonical offsets 推導的狀態。每個 central／local field 的 exact compare
仍保留。Canonical EOCD 為最後 22 bytes、comment 必須為零，移除通用 ZIP tail search。
writer 仍使用 stdlib `ZipFile`；沒有改圖片解碼、壓縮、縮放或來源預檢政策。

| 經重新確認的責任 | 決策／位置 |
| --- | --- |
| Artifact policy／values、image、archive、source preparation | 已完成 `artifact/model.py`、`images.py`、`archive.py`、`renderer.py`；`_streams.py` 是共用 bounded byte I/O，`__init__.py` 為原公開 API 的唯一入口，沒有 forwarding functions |
| Ingest `presentation/` 暫名 | 改用既有 `artifact/` 公開名稱作 canonical package，不新增另一層相容 shim，也不新增 PyPI 發行單位 |
| Source／qualification | `image_qualification.py` 屬 source eligibility，引用 images/model owner；未搬進 renderer。`source_monitor`／`source_schedule` 屬 resident 排程，未把它們混入純 observation 子套件 |
| Library | 保留 staging／journal／activation／cleanup／relocation 的完整生命週期責任；後續以 I/O ownership 為單元，不能直接合併不同恢復語意 |
| Core identity／codecs | 純函式邊界仍成立，但固定 bytes／digest 契約與廣泛 callers 需一起驗收；目前單純搬檔不能證明收益，留在 Core，不先拆 PyPI |
| Devtools | Core probe 直接 hook 新 owner，並同步相同 wrapper 到實際 import aliases；舊單檔 artifact 私有 hook 不再支援，instrumented 驗收需本輪 Ingest candidate |

| Python 實體行數（相對本輪兩庫基線） | Runtime | Tests | Dev tooling | Generated |
| --- | ---: | ---: | ---: | ---: |
| Ingest | -85 | +142 | 0 | 0 |
| Core | 0 | +97 | +13 | 0 |
| 合計 | -85 | +239 | +13 | 0 |

總 Python 淨增 **167**，不是總減碼。單純分檔時 artifact 1,943→2,067（+124，
主要是 imports／模組邊界）；真正刪除重複解析後為 1,860，另 qualifier imports -2，
最終 runtime -85。Artifact 最大單檔 1,943→816。維護收益是 archive 只剩一份
解析規則，圖片與 preparation 不再放在同一巨檔；不以搬檔或測試新增冒稱刪碼。

實驗與回歸：

- 原 parser 接受缺少 DEFLATE EOF、trailing／concatenated streams、實際解壓長度
  超過宣告值／metadata cap 等損壞輸入。新版用 `declared + 1` output cap，驗證
  EOF、unused／unconsumed input、exact size 與 CRC；64 MiB expansion 的案例要求
  Python peak allocation 小於 8 MiB。原 body 負向控制 7 個預期失敗／4 通過；新版
  對應 11 通過，另外新增 EOCD 截斷／comment／trailing 三種拒絕案例。
- 同次 inspection 的 metadata／兩頁 local-header read 由 `(4, 3, 3)` 變為
  `(1, 1, 1)`；沒有把此局部次數當作端到端效能提升。原 47 個 top-level definitions
  中 38 個 AST 完全等價；27 個公開 exports 保留。
- 實際 writer 差分四組全部通過：metadata-only；RGBA PNG／EXIF rotated JPEG／GIF；
  自訂尺寸品質與 BICUBIC／parallel；NEAREST／preparation cache。CBZ、各 JPEG、
  thumbnail exact bytes、policy fingerprint 與三種 evidence 全部相等。暫存 runner
  `compare_writer_bytes.py` SHA-256：
  `5b24045e1bf105abb7e46e502a033a2b77a33be3ed053d90401995148998ca80`；
  report `writer-bytes-differential.json` SHA-256：
  `c31cc77f0e92768b0d84d7f30525ba5d22a31d89113aae2afc04961876f82c39`。
  兩者位於 `/private/tmp/h2hdb-artifact-refactor/`；baseline 由 `c5a6967:src/h2hdb_ingest/artifact.py`
  匯出，candidate 為 `a92747c`，以 Ingest `.venv/bin/python` 執行 runner。
  證據遺失時依上述四種形狀與來源重驗，不能只沿用暫存路徑。
- 新依賴環境的 `tests/test_artifact_vnext.py` 214 passed；先前 artifact／preparation／
  source images／progress 選集 265 passed、2 deep deselected；qualification／runtime／
  library 選集 121 passed，加上因 sandbox 禁止 `ps` 而失敗的五個程序案例在取得
  process inspection 權限後 5 passed；沒有修改測試或放寬程序清理要求。
- Core probe tests 35 non-MariaDB、3 MariaDB passed；新增 regression 確認 import aliases
  使用同一 wrapper。真實 installed-module smoke 的 instrumentation 前後 bytes/evidence
  相同，hash 4×547=2,188 bytes、source verification 292 bytes，counters 無漏計或重複。

乾淨環境由 `uv pip install --refresh --python VENV/bin/python CORE_WHEEL
h2hdb-ingest[dev]@file://INGEST_WHEEL` 正常解析 43 packages，沒有 `--no-deps`；
`uv pip check` 通過。Core `0.45.8` wheel hash 與 R02 相同，Ingest `0.30.4` wheel
SHA-256 為 `d3f876c29ea0d429ae69ab8a023b1c25ee173d33e0528ce8a7c2b04b2e1378ef`。
Core 98／Ingest 54 個 package files 的 source、wheel、installed bytes 與 METADATA
相同，`direct_url.json` 指向明確 candidate wheel。此 wheel 替代 index Ingest；
不能據此宣稱 index-backed Compose 已可部署。

Core 使用此環境的 `H2HDB_ACCEPTANCE_PYTHON`，另設 `H2HDB_TEST_MARIADB=1` 執行
`.venv/bin/python -m pytest tests/test_deployment_acceptance_fixture.py -n 0
--check-backend-pairs -m '' -k test_real -q`：6 passed／32 deselected，117.68 s，
沒有 skip。包含 SQLite／MariaDB native parent oracle、真實 CBZ／restart 及兩種
mixed raster 配置；不是只有 import 或版本字串驗證。

Ingest 手動 release validation：`H2HDB_TEST_MARIADB=1 .venv/bin/python -m pytest
-n 0 --check-backend-pairs -m mariadb tests/test_runtime_e2e.py` 分兩個不重疊 selection
執行，`-k 'not large_encoded_source'` 為 27 passed（508.30 s），
`-k large_encoded_source` 為 1 passed（15.01 s），完整 28 unique cases、零 skip。
包含 257 頁 CBZ、100 MP mixed repair、33 MiB source、restart／policy takeover／
relocation／replacement／storage pressure；測試前後 source hashes 相同。
此處 Core 為 index `0.45.8`，Ingest 實際 source/project 為 `0.30.4`，舊 editable
metadata 尚為 `0.30.3`；不是前述乾淨 wheel 的證據。命令、環境與 source hashes
保存在 `/private/tmp/h2hdb-artifact-mariadb.YvmqeI/environment.json`，SHA-256：
`520cf98c013d787aec29c95637f23f76a6073e1af7f7c32517e7e22770cf6a03`。
JUnit `results.xml`／`large-results.xml` SHA-256 分別為
`cdc5803e1ff0c15d458d01145865fbf40ade025ec55d00a3b0ddfa168d972c90`／
`4d90b848082a9048baa56d9a729ba409ac55bc3820f66d67e7739d73e29af469`；
來源為上述 `a92747c` runtime，可按相同兩個 selections 重建證據。

Ingest 已經 `scripts/git-flow-merge.sh` 合併至 `main`，merge `3ce26cd`，task branch
已刪除。實際 pre-merge hook 的 version／full profile 通過：Ruff、formatter、strict
mypy、Markdown、backend collection、bounded pytest 2,033 passed／7 skipped
（112.46 s）、Lean、TLC Small、sdist/wheel 與 installed-wheel smoke。七個 skip 是
兩個 private-corpus opt-in 與五個 Windows-only 案例，不當成已驗證。
Ingest 此版本使用 hook 直接執行完整檢查，沒有 Core 式 exact-tree receipt；不要
混稱兩庫證據。Ingest 獨立唯讀審查 `c5a6967..e3e6463` 未發現 P0／P1／P2，
Core 正式 online review 仍由其 merge flow 執行。

本輪以品質優先選擇完整 parser ownership，未受最小修改或向後相容限制；受支援
runtime 公開 API／canonical bytes／schema／資料格式為「品質優先後恰好向後相容」。
公開 inspection 現在拒絕上述原本誤接受的損壞 metadata，屬於原 canonical 契約修正；
沒有移除 runtime compatibility path，也沒有新增 shim。Core dev probe 的私有
instrumentation 接點改為本輪 Ingest 結構，不能搭配舊單檔 artifact 的 `0.30.3`；
必須同步使用 `0.30.4` candidate，沒有 fallback。這是驗收工具的版本配套限制，
既有契約未承諾支援任意歷史 Ingest，公開 CLI、事件名稱與 counters 都未改，
不將它分類為公開 breaking change。所需操作是同步工具與套件；不需要一次性
資料轉換，不涉及資料遷移、刪除或重建，既有資料全數保留。
Ingest shipped runtime impact 為 patch，`0.30.3`→`0.30.4`；Core 只有 dev tool／test／
ledger，impact none，維持 `0.45.8`。Ingest audit 已重查並人工審閱 Core `0.45.8`、
Pydantic `2.14.0`、Hypothesis `6.168.5`、mypy `2.4.0`、Ruff `0.16.10`，本機及乾淨
環境驗證新版；其餘直接依賴最新版不變，所有 bounds 滿足，不修改 dependency range。
Core 正式 online review 無 findings；full gate 全部通過，包含 4,735 non-MariaDB
passed／9 skipped、24 MariaDB smoke passed、Lean、TLC Small 與 distribution boundary。
九個 skip 是三個需 explicit Ingest／Pillow 的案例與六個 Windows-only；前者另以
上述六個雙 backend real cases 驗證，後者未在本機執行。Merge `1010ec2` 已整合
`c03efe5`／`05cc07f` 至 `master`，task branch 已刪除，exact-tree full receipt 位於
Git metadata；本段措辭澄清另走 documentation profile，不冒稱再次執行 full gate。
沒有 schema、清理／compaction／交易快取或部署組合變更，未重跑
完整 deep、效能矩陣、cleanup-acceptance、Compose build／隔離部署流程或 production
驗收；不宣稱端到端加速或正式可部署，沒有 push、publish 或 deploy。

## 2026-10-10 log-4：清理階段與接續排序

來源為使用者提供的 `/Users/kuanlun_wang/Downloads/exhentai-h2hdb-ingest-4.html`，
36,267,767 bytes，SHA-256：
`74185d4e82a40eaa0fcccad5ca6916e8a83132d97e6b519d3e6b790d389be6c4`。
Startup 明列 Core `0.45.8`、Ingest `0.30.4`、MariaDB，版本符合上述變更後版本；
log 不能證明 installed bytes 等於本機 checkout 或構成相同工作負載的前後比較。

重建方法：依 HTML 表格讀取 4,990 個 rows 並反轉為時間正序，解碼 entities，
將不含新事件時間／level 前綴的 continuation 接回上一事件，保留 JSON 內容；
得到 3,676 events，1,410 個 database JSON 全數可解析。只統計 unique operation ID
的 completed roots（1 個 audit、1,313 個 cleanup），不累加 95 個 progress snapshots。
逐筆核對 root exclusive 加所有 phase exclusive 等於 elapsed，最大誤差為零。
第二次獨立 HTML 解析得到相同 rows；本文數值可按原檔與以上規則重新計算，
不以暫存分析程式的存續作為證據前提。

| 不重疊的壁鐘區間（log 時間） | 所需時間 |
| --- | --- |
| 01:44:59–01:45:09.204，啟動至選定 audit | 約 10.204 s，起點僅秒精度 |
| 01:45:09.204–03:06:18.313，完整 audit | 4,869.109 s＝81 分 9 秒 |
| 03:06:18.313–03:07:05.910，啟動尾段，含第一次 cleanup | 47.597 s |
| 03:07:05.910–15:51:21.294，resident maintenance | 45,855.384 s＝12 小時 44 分 15 秒 |

全段約 14 小時 6 分；沒有進入 ingestion、render 或 publication，排除圖片
壓縮／縮放／預檢後仍是這段 foreground 工時。沒有 WARNING／ERROR 不代表
達成完整流程或成本目標。Audit 因 validators changed 啟動，其 DB scope
4,868.837 s、4,221,910 SQL calls、SQL 2,810.351 s；semantic validators exclusive
4,287.349 s，最昂貴單一 validator `catalog.discovery-exactness.v1` 為 1,952.327 s。
Core `database_audit._validator_version()` 包含 project version，不是 validator
原始碼的 fingerprint；因此此原因不能證明本次實際修改了 validator。後續升版也
可能要求 full audit，分割版本的部署成本須另評估，不在本輪放寬 audit 契約。

Cleanup 1,313 次全部 `PROGRESSED`、零 `DONE`，每次 16 batches，共 21,008
committed batches、182,269 committed logical rows。Logical rows 不是 gallery 數、
physical deletes 或剩餘工作量。Cleanup active elapsed 合計 45,450.753 s，含啟動
尾段第一次工作；這是上述壁鐘的子集，不能另外相加。809,602 SQL calls 花費
44,743.871 s，占 cleanup active **98.44%**。

| Cleanup exclusive phase | 秒 | Cleanup active 占比 |
| --- | --- | --- |
| 全部 maintenance eligibility | 39,340.733 | 86.56% |
| 其中 CANONICAL_VALUE | 25,021.064 | 55.05% |
| 其中 CONTENT_BLOB | 6,545.766 | 14.40% |
| 其中 FILE_NAME_IDENTITY | 2,622.514 | 5.77% |
| cleanup_phase | 4,009.350 | 8.82% |
| next_cycle 自身 | 1,742.575 | 3.83% |

前三個 eligibility targets 合計 75.22%。CANONICAL_VALUE 為 4,636 SQL calls；
後兩者各為 4,636 phase calls、1,567 SQL calls，已存在 absence proof 重用，
不能把 phase 次數當成 SQL 次數或宣稱尚未有快取。SQL parent／phase 成本不可重複加總。
依時間等量分四組，logical rows／active second 為 4.171→4.121→3.917→3.826，
每 batch rows 為 9.304→8.829→8.396→8.177；CANONICAL_VALUE 每 SQL 約
5.481→5.347→5.372→5.384 s，沒有持續變慢的證據。每次都有 committed rows，
不是已證實的無進度死循環；吞吐變化伴隨每批有效工作量下降。

Ingest `resident.py` 在 Core 回傳 `PROGRESSED` 後直接回報 maintenance，未走
`try_claim_ingest`，符合紀錄。Filesystem `library_cleanup_io` 最後累積 snapshot
僅 24.072 s／1,312 calls，全部 library outcome DONE，不能當成 DB cleanup DONE。
487 次背景 source inventory 每次觀測 132,277 markers，中位數 61.044 s；它和
foreground 並行，不另加壁鐘，也不能從 logical bytes 推定實體磁碟工作量。

結論：足以選定 cleanup eligibility SQL 為效能熱點，無須再固定等一小時。
尚無初始 backlog、剩餘候選總量／可推算的 durable 進度分母、query plans 與
資料形狀，無法估清完 ETA 或可信的理論最佳時間。即使不切實際地將 eligibility
全部降至零，也仍剩 6,110 s；約 7.44 倍只是該假設的加速上限，不是可達承諾。
後續應以合成、可拋棄的代表 workload、既有成本預算與退化反例驗證可省工作，
不直接對 production 執行 SQL probe，也不因觀測結果調高預算。

R01／R02／R15 對應的 ingest／CBZ 路徑未執行，不能判定其加速成功或失敗。
若要驗證端到端，需看到 **DB cleanup DONE → next claim → 完整 ingestion／
publication／activation → 後續 cleanup DONE**；缺乏剩餘量，現在不能給等待時數。
81 分鐘 audit 屬獨立責任，不因 cleanup 分割就視為解決。

## 下一輪入口

先重核目前快照的七個來源、涉及庫政策、Git ancestry 與既有驗收；新變更須重驗
受影響結論。下一單元為 **Core cleanup 完整內部分割**，不先要求可大量減行。
依 Core `c583a89` 的責任盤點，建議下列 owner；檔名於實作前核對 import DAG：

| 內部 owner | 現有責任與來源 |
| --- | --- |
| `_ingest/maintenance.py` | `vnext_ingest_facade.py` current-only attempt：lease／最多 16 次交易、adapter release、outcome 與補償；facade 保留公開入口 |
| `_cleanup/model.py`、`cycle.py` | `vnext_cleanup_repository.py` cycle values、checkpoint／receipt／replay／completion；維持單一 durable cycle owner |
| `_cleanup/selection.py`、`eligibility.py` | Maintenance classification、open-cycle resume、target／shard 選擇；既有 absence proof 的 attempt ownership |
| `_cleanup/static.py`、`roots.py` | Bounded keyset／delete／cursor、bind／row budget、frozen root load／freeze／validation |
| `_cleanup/targets/`、`registry.py` | Source-owned closed plans 與各 family retention SQL／mutation／recovery，publication commit 恢復作完整單元 |

依賴由 facade → maintenance → selection／cycle → registry → targets → static／roots
→ model；cycle 傳入已選 plan，底層不得反向讀 registry。這是待實作的責任邊界，
不是已驗證的最終檔案分配。Canonical persistence 是多 workflow 共用的低層 owner，
不移入 cleanup；Ingest filesystem lifecycle 也留在 adapter。

同 connector／managed transaction、exact fencing、bounded child-first reachability、
response-loss replay 及 transaction 外 adapter I/O 必須維持；分割不延長 transaction
local operation 或 attempt absence proof 的生命週期。Audit scheduler／validators
維持獨立 oracle，不直接借用 writer eligibility 代替其判斷。
同步更新 `catalog_writer.py` 的 installed method identity 登記、physical domain／
state-machine bindings、`verification/invariants.toml`、測試 patch 接點與 cleanup
成本工具的 private instrumentation；移除舊 private owner，不保留轉接 shim，
不放寬 writer 登記以遷就任意 free functions。

驗收包含受影響雙 backend cleanup／lease／reconnect／replay／READY tests、
`scripts/run-pytest.py cleanup-acceptance`、正式 review／full gate 與 distribution。
核對 Ingest resident／progress／e2e consumers；涉及跨 ingest 發布／清理流程時，
按原政策補實際 Compose 衍生的隔離 `--instrumented --cleanup-faults` 驗收。
分割成果、LOC、效能各自回報，R03 成本未經相稱實測通過仍未完成；只加工具
或只寫 docs 不算完成此分割單元。發布／部署不在本輪授權內。

## 重查後仍未完成

下表為 2026-10-10 的來源與責任重查結果；未宣稱逐檔重新審核所有未變模組。

| 範圍 | 狀態與下一步 |
| --- | --- |
| C05／C04 cleanup，R03 | 下一單元：上述完整內部分割；另定位 eligibility SQL、工作量與 durable 進度，既有成本違反未解決 |
| C07 READY audit，R03 | 仍需：scheduler／validator ownership 與約 81 分鐘成本獨立調查；不能由 cleanup 結果結案 |
| C02／C04 analysis，R16 | 順序後移：完整 run／authority／preparation／file／content／GID／snapshot ownership；各 family 擁有 BUILD／VALIDATE／replay，shared batch 不反向 dispatch family |
| 其餘 Core，含 C08 codecs、publication 與 canonical persistence | 仍需邊界審核；codecs 可內部分組但控制 persisted bytes，canonical persistence 不歸 cleanup |
| I03 library／R07、I01 source 與 resident 編排 | 仍需完整 lifecycle 分割；小 JSON／hash helper 隨 owner 收斂，不單獨作預設工作；qualification 與 schedule 不全塞入純 source engine |
| C01／C04 source SQL，R14 | 已知成本未達標，仍需按原契約驗證；本 log 未進入 ingest，沒有新修正證據 |
| R01／R02／R15 | 已完成當輪範圍；受影響契約改變時重查，保留現有 artifact package，不為換名重搬 |
| S2：OPDS、Komga、Downloader、hbrowser，R08–R11 | 仍需各自 reader fencing、protocol mapping、batch／root／browser resource ownership 審核 |
| S3：部署 P01–P07／R12、工程工具 E01–E02／R13 | 仍需逐責任審核與工具分類；部署控制來源未變不代表 runtime 可部署 |
| S4／R04–R06 PyPI | 獨立發行暫緩至使用／發布邊界及淨收益成立；全系統最終重查仍未完成 |

目前無任何完整 C／I／O／K／D／H／P／E 分組可宣告全部審核完成。
每輪修改後重查受影響待辦，說明仍需、順序改變、已涵蓋或拒絕／延期與重開條件；
最終回覆列具體剩餘項目，不能只寫「全系統尚未完成」。

## 每輪交付與證據

- 一個完整功能邊界的問題、owner／依賴、行為變化、假設、反例及設計取捨。
- 涉及修改時，記錄實作 commit、影響範圍及實際執行的既有檢查；未跑、skip 分開列明。
- 明列刪除的重複實作／狀態／責任及保留原因，分開計算 runtime、tests、dev tooling、
  generated code 增減；揭露總差異和新增抽象成本，不能只以搬檔、拆包或加速宣稱精簡。
- 效能主張附事前成本契約、中大型資料形狀、固定／逐筆成本及 baseline／candidate provenance。
- Ledger 只保存摘要和證據索引：repo／ref、相對路徑或 artifact ID、hash、命令與結果。
  大型 logs 不複製進本文；缺失證據記為待重新取得，正式 receipts 不由本文取代。
- 本輪完成前回寫候選、範圍審核狀態與下一個入口；只改 ledger 不代表 runtime 通過。
  實作 commit 可先記錄，最後 merge 由 Git history 核對，避免自我引用 commit hash。

預設一輪完成一個功能邊界的完整處置：先說明具體修改方案、預期收益及驗證方法，
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
5. 內部模組化與獨立拆包各自有決定及依據；說明實際消除的重複責任和維護成本，
   列出分類 LOC 與總增減。不能只以減行數判定完成，也不能隱去沒有減碼的結果。

目前進度：R01、R02、R15 已整合並重核 ancestry；下一輪先完成 cleanup 內部分割。
最新 log 只涵蓋 startup audit／maintenance，不是 ingest 端到端效能驗收。
具體剩餘工作以上表為準，S1–S4 及全系統尚未完成。
