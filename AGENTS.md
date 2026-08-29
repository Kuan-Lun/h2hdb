# AGENTS.md

## 政策來源

- 本檔是此 repository 的唯一代理開發政策來源。
- 其他代理入口只能要求完整閱讀本檔，不得複製另一份政策。
- 可執行規則以 repository 內的 scripts 與設定檔為準。

## 溝通

- 最終回覆一律使用繁體中文。
- 程式碼、識別字、命令、檔名與 commit message 可使用英文。
- 不得為了承載回覆而新增 Markdown 文件。
- 移除 compatibility path、改變公開行為或採用例外時，必須在對話及
  最終回覆中明確說明。

## 設計與修改原則

- 不預設存在最小修改或向後相容要求。
- 在任務範圍內選擇架構、可讀性與可測試性最好的完整結果。
- 綜合考慮 SOLID、KISS、YAGNI、內聚性與低耦合。
- 必要的局部重構可直接納入任務。
- 若會實質擴大任務範圍、改變原要求未涵蓋的公開行為，或引入資料遷移，
  必須先取得使用者同意。
- 任務直接涉及的 legacy compatibility code 應移除，不保留 shim；不全面
  清理與任務無關的 legacy code。
- generated output 不得直接修改；必須修改 generator 或 source 後重新產生。

## 工作樹與 Git

- 唯讀分析不建立 branch。
- 凡會修改 tracked files 的任務，使用
  `scripts/detect-primary-branch.sh` 判定 primary，並建立專用 task branch。
- 不得 stash、reset、clean、覆寫或混入既有使用者修改。
- 工作樹不乾淨時，從 committed primary 建立獨立 worktree。
- task branch 可包含多個邏輯 Conventional Commits。避免巨大 commit；小而
  內聚的任務仍可只有一個 commit。
- 任務完成後執行 `scripts/git-flow-merge.sh`。該腳本負責 exact-tree release
  gate、`--no-ff` merge、安全移除 task worktree，以及以 `git branch -d`
  刪除已合併的本機 branch。
- task branch 與 primary 只須有 common ancestor；不得要求 task branch 必須
  包含 primary 的最新 tip。分歧由正常 three-way merge處理。
- merge conflict 或 gate failure 時必須 abort merge並保留 task branch。
- merge 後收到的任何 follow-up 都建立新的 task branch。
- 本機 task branch、commit、`--no-ff` merge與 `branch -d` 已獲預先授權。
- fetch、pull、push、remote branch、tag、release、publish、deploy與任何
  force操作仍須逐次明確授權。
- 不得使用 `--no-verify`。

## 提交格式

- 所有非 merge commit 必須符合 Conventional Commits。
- Breaking change 使用 `type!:` 或 `BREAKING CHANGE:` footer。
- project version 更新使用獨立 commit：
  `chore(release): bump version to X.Y.Z`。

## 版本政策

- `pyproject.toml` 的 `[project].version` 是唯一 project version source。
- project version 固定使用 `X.Y.Z`。
- 歷史 legacy `0.23.0.11` 只容許在本次 policy bootstrap 正規化為
  `0.23.1`；後續不得再使用四段版號。
- 1.0 前，`Y` 是 compatibility lane，`Z` 是同一 lane 內的相容 release
  counter。相容修正或功能遞增 `Z`；breaking change遞增 `Y` 並將 `Z`
  歸零。
- 1.0 後使用標準 Semantic Versioning。
- 整個 task branch只在整合前更新一次 project version。
- shipped runtime、schema或 deployment surface 有變更時，至少需要相容
  升版。
- Breaking API、CLI、config、schema、protocol、資料格式或 Python/platform
  support變更必須提高 compatibility lane或 major。
- tests、一般文件、IDE、hooks、CI與 dev-only tooling 單獨變更時不升版。
- 未分類路徑必須明確判定 impact，不得靜默當作 `none`。
- 已證實不改 artifact或行為的格式化、註解或重構可在 task commits加入
  `Version-Impact: none` 與非空白的 `Version-Reason:`；最終回覆也必須揭露。
- `scripts/check-version.py` 以整個 task相對 primary的差異判定一次，不逐
  commit升版；merge candidate使用 staged index tree判定。
- project version變更必須包含由 `scripts/audit-dependencies.py` 產生且已
  人工審閱的 `.release/dependency-audit.json`。

## 依賴與環境

- repository 必須能從單一乾淨 checkout重建，不得依賴固定 sibling clone
  路徑。
- 明確跨 repository任務可使用傳入的 wheel、Git URL/ref或 repository
  path；sibling discovery只能是選擇性的效能優化。
- Python registry dependencies原則上使用 `>=` lower bound；合理 upper
  bound與 `!=` 可以保留，但必須有相容性依據。
- 精確版本只允許經驗證且有文件理由的特殊契約。
- dependency audit必須涵蓋 build、runtime、optional、development與 Node
  direct dependencies，並記錄現有 upper bound之外的 registry最新版。
- audit script負責盤點與候選發現；有新版時仍須檢查 release notes、驗證
  相容性並嘗試修正問題，再以具體 review note產生 receipt。
- audit receipt綁定 project version與 dependency manifest hash；它本身不
  取代完整測試。exact-tree release receipt會連同 audit evidence一起綁定
  merge candidate。
- `uv.lock` 與 `package-lock.json` 不得成為 committed或重建輸入。
  `scripts/rebuild-env.sh` 可使用 `uv venv` 與 `uv pip`，但不得使用會依賴
  project lockfile的同步流程。
- Node tooling使用 `npm install --package-lock=false`。
- 不得依賴 system-wide lint、format、type-check或 Markdown工具。
- `requires-python` 使用 `>=3.14`；只有經驗證的壞版本可使用 `!=`。

## 品質工具

- `pyproject.toml` 是 Ruff與 mypy的唯一規則來源。
- 使用 Ruff lint與 Ruff formatter，不使用 Black。
- Ruff使用適合本專案的嚴格規則集，不從 `ALL` 出發；每個停用規則必須
  在設定旁記錄理由。
- mypy使用標準 `strict = true`。不得保留 `mypy.ini`。
- module例外使用精確 TOML overrides。
- `type: ignore` 必須指定 error code並附理由。
- `noqa` 必須指定 rule code並附理由。
- Markdown使用 repository-local `markdownlint-cli2`。
- VS Code使用同一份 `pyproject.toml`、Markdown設定與 `.venv`；CLI gate是
  最終權威，IDE diagnostics為即時輔助。

## 檢查與 release receipt

- `scripts/format.sh`：明確執行會修改檔案的 Ruff fixer、Ruff formatter與
  Markdown fixer。
- `scripts/check-fast.sh`：離線、唯讀的 Ruff、format check、strict mypy與
  markdownlint；每次非 merge commit執行。不得寫入 Ruff或 mypy cache。
- `scripts/check-full.sh`：fast gate、coverage contract、schema與 generated
  artifact drift、schema surface、Lean、完整 SQLite/MariaDB 10.11.11 tests、
  small TLC profiles，以及 distribution boundary。
- deep TLC只供明確的手動驗證，不進入自動 merge gate。
- `.githooks/pre-merge-commit` 透過 `scripts/release-gate.py run --index`
  驗證 staged candidate；不得另建競爭的第二套 merge gate。
- release gate先驗證 task-level version與 dependency audit，再呼叫
  `scripts/check-full.sh`。成功 receipt存在 Git metadata，且只對 exact tree、
  project version、gate profile與 required-check set有效。
- exact-tree release receipt不得 commit、修改或偽造。相同 tree的 merge
  commit與 pre-push可以重用 receipt；tree或 required checks改變就必須重跑。
- dependency audit可連網；commit hooks只驗證 candidate內既有 evidence，
  不在一般 commit過程連網。
- GitHub Actions只保留 trusted publishing、手動 formal profile、平台特有
  或本機無法可靠重現的檢查。
- 不使用 Claude、Codex或其他 provider-specific Stop hooks重複檢查。

## 測試與例外

- runtime行為變更必須新增或更新測試；bug fix必須有 regression test。
- 新功能涵蓋正常、邊界與錯誤路徑。
- 數值測試固定隨機種子；容許誤差需有依據。
- flaky test視為失敗，不得以重跑掩蓋。
- 不設定跨 repository的統一 coverage百分比。
- live account、network、production或 destructive probe不得進入 hooks、
  一般 pytest或自動 merge gate。
- `skip` 或 `xfail` 必須有理由；`xfail` 原則上使用 `strict=True`。
- 不得為通過檢查而全域放寬工具設定。

## 完成回報

最終回覆必須包含：

- 實作及公開行為變化。
- 移除的 compatibility path。
- project version、version impact與 dependency audit結果。
- commits與完整檢查結果。
- primary branch與 merge commit。
- branch/worktree是否已清除。
- 是否仍未 push、publish或 deploy。

## Repository-specific scope

`h2hdb` 是 SQLite/MariaDB database、coordination與 revision catalog core。
本 repository擁有 greenfield schema epoch、normalized catalog與 operational
relations、bounded transactions、durable coordination state，以及 public
application facades。

- Core不得依賴 Pillow、FastAPI、OPDS types、`hbrowser`、filesystem scanning、
  gallery parsing或具體 CBZ/object-storage行為。那些責任屬於 consumer或
  adapter repositories。
- Consumers只使用 `VNextDatabaseAdminFacade`、`VNextCatalogFacade`、
  `VNextIngestFacade`、`VNextDownloadQueueFacade`與公開 immutable values，
  不得直接使用 connector、repository、generated schema或 table internals。
- 不得重新加入 `H2HDB`、`MigrationRunner`、numbered migration ledger、legacy
  hand-written catalog repositories、compatibility view或 dual-write path。
- Public administration與 catalog-opening entry points只能使用 wheel-resident
  generated schema provider。不得加入 caller-injected provider作為第二個
  schema-authoring surface或 production test seam。
- 易漂移的 relation與 bootstrap row數量不寫死在本檔；manifest、generator與
  executable checks才是 authoritative evidence。

## Manifest-first schema workflow

Logical authoring surfaces為 `verification/schema/catalog.toml`與
`verification/schema/operational.toml`。它們對宣告的 functional dependencies
採 closed-world解讀；遺漏 semantic dependency即使 checker通過仍代表設計
主張無效。

Schema變更依序進行：

1. 在 logical manifests加入或修改所有 relation、key、functional dependency、
   decomposition、bootstrap fact、semantic obligation與 materialization理由。
2. 由 repository generators重新產生 `physical.toml`、
   `operational_physical.toml`與 catalog/operational Lean schema files。
3. 重新產生 wheel-resident `_generated_vnext_schema.py` provider artifact。
4. 實作或更新 manifests指定的 validators、writer bindings、repositories與
   fault/integration evidence。
5. 在使用新 manifest前跑 schema、Lean、coverage metadata、runtime與兩個
   database backends的 checks。

- 不得手改 generated physical manifests、generated Lean schema files或
  generated runtime provider。
- relation shape變更必須同步反映在 manifests、checks與長期架構文件。
- BCNF與 physical width是不同 gates。一般 physical `catalog_*` base table只
  包含 semantic primary key與最多一個 atomic non-key value；例外的完整
  wide shape必須由 manifest明確審核。
- 不得把普通值藏入 primary key、packed scalar、JSON或 EAV，只為了讓 table
  width或 relation count看起來更小。

## Formal verification

- `verification/invariants.toml` 是 catalog與 operational contracts中所有
  `semantic_obligation` ID的 closed-world evidence index。
- 新 obligation必須依性質提供真實的 FD、Lean、TLA+、runtime refinement、
  fault或 integration evidence；gate拒絕 missing IDs、stale symbols與把有限
  TLC探索描述為無界證明的主張。
- `coverage --validate-only` 驗證 evidence contract並回報 production blockers；
  plain `coverage` 才是 strict production-readiness gate。不得把 schema
  generation、Lean或 metadata validation的成功誤稱為 production coverage。
- TLC只窮舉所選有限 constants的 reachable states。Lean theorem只對明列的
  mathematical inputs與 assumptions無界成立，不自行證明 Python、SQL、
  transaction或 filesystem effects refine模型。

## Architecture and transaction rules

- Protocols放在 `ports.py`；neutral data放在 `domain.py`。
- Backend-specific行為留在 `SQLConnector`後方。共用 SQL使用 `%s`，SQLite
  connector負責轉成 `?`。
- 每個 cross-table workflow共享單一 connector與 managed transaction。
  Internal unit-of-work可以協調 repositories；consumer不得呼叫 internal API。
- SQLite writes使用 `BEGIN IMMEDIATE`；MariaDB在 fencing、allocation或 epoch
  serialization需要時使用 row/advisory locks。read-only mode由 connector
  強制。
- 不得信任 caller提供的 digest、derived ID、count、cursor、name、generation、
  lease或 token。必須重新計算或載入 durable authority，mismatch時 fail closed。
- 任意長資料使用 bounded canonical pages與 streaming validation。Batch必須
  hard-capped、keyset-paged、idempotent且能承受 response loss。
- Immutable identity、history、event、receipt與 publication facts不得原地
  update；mutable state隔離於 normalized heads、owners、leases、checkpoints與
  明確 state-machine relations。
- Publication與 coordinated completion只在短 transaction內驗證 sealed scalar
  state，不得掃描無界 source、projection、artifact、queue或 event集合。
- Exact attempt/generation/token fencing必須阻止 delayed retry完成或修改替代
  work。
- Cleanup必須 bounded、child-first且檢查 reachability；任何仍被 active work、
  publication、pending effects或 protection claims引用的 identity/history都須
  保留。
- Catalog calls只接受 current publication head。 supplied `CatalogRevision`只在
  仍精確等於該 head時有效，每次 read回傳前都須重查 head。
- 在 manifest定義 normalized current-head index前，nonblank search維持不可用。
- 在 manifest定義 durable revision-scoped authority與 replay semantics前，
  不得從 transient joins推導 `redownload_required`。
- Public ingest orchestration把 transaction-owned issue/commit與 adapter-owned
  local preparation分開；filesystem與 object-storage不得進入 core DB
  transaction。

## Schema epoch and backend rules

- 只有本 repository擁有 schema。CLI對 epoch 3/version 1只公開 `migrate`、
  `check`與 `ready`。
- `migrate`只接納真正空白 database，寫入 checksum-bound `BUILDING` marker，
  套用 idempotent generated DDL/bootstrap slices，驗證 exact manifests後轉為
  `READY`。
- Interrupted run只可恢復相同 manifest-bound `BUILDING` epoch；`READY`重跑
  只驗證，不修改 data-plane schema。
- Previous、foreign或 drifted database必須拒絕；重建從新的空 database開始。
- 每個 production SQL relation identifier都必須由 `physical.toml`、
  `operational_physical.toml`或唯一 epoch-control relation接納。Formal BCNF通過
  不代表可以發布第二套未 manifest的 SQL schema。
- `check`在 read transaction執行完整 `READY` audit；`ready`是 O(1) read-only
  epoch/version/manifest probe。Provider blocker必須在開啟或修改 database前
  fail；consumers不得初始化 schema。
- Shared schema、transaction、connector、validator或 repository變更都須測
  SQLite。MariaDB cases以 `H2HDB_TEST_MARIADB=1`啟用 testcontainers，並固定
  MariaDB 10.11.11，對應 Synology package build 10.11.11-1551。
- Docker不可用時必須精確回報；不得把未執行 MariaDB測試描述為通過。
